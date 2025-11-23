use anyhow::{Context, Result};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use borsh::BorshDeserialize;
use csv::WriterBuilder;
use dotenvy::dotenv;
use reqwest::Client;
use solana_accounts_db::append_vec::AppendVec;
use solana_sdk::pubkey::Pubkey;
use std::{env, fs::File, io::{BufReader, Read}, path::Path};
use tar::Archive;
use tempfile::NamedTempFile;

// Metaplex
use mpl_token_metadata::accounts::Metadata as MplMetadata;
// SPL Token (classic)
use spl_token::state::Mint as SplMint;

const METAPLEX_OWNER: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";
const TOKEN_OWNER:    &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Clone, Debug)]
struct Row {
    token_address: String,
    name: String,
    symbol: String,
    token_uri: String,
    is_mutable: u8,
    update_authority: Option<String>,
    mint_authority: Option<String>,
    freeze_authority: Option<String>,
    protocol: u8,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    dotenv().ok();

    let ch_url       = env::var("CLICKHOUSE_URL").context("CLICKHOUSE_URL missing")?;
    let snapshot_path= env::var("SNAPSHOT_PATH").context("SNAPSHOT_PATH missing")?;
    let batch_size: usize = env::var("BATCH_SIZE").ok().and_then(|s| s.parse().ok()).unwrap_or(50_000);

    // Optional: external protocol map CSV: "pubkey,protocol"
    let protocol_map_path = env::var("PROTOCOL_MAP").ok();
    let ua_map = load_protocol_map(protocol_map_path.as_deref());

    // 1) Which rows are incomplete?
    let wanted_mints = fetch_incomplete_mints(&ch_url).await?;
    println!("Found {} incomplete token rows", wanted_mints.len());
    if wanted_mints.is_empty() {
        println!("Nothing to backfill.");
        return Ok(());
    }
    let wanted: HashSet<Pubkey> = wanted_mints.iter().filter_map(|s| s.parse().ok()).collect();

    // 2) Scan snapshot offline
    let rows = scan_snapshot(&snapshot_path, &wanted, &ua_map)?;
    println!("Collected {} rows from snapshot", rows.len());

    // 3) Write to CSV and bulk-insert
    let csv_path = write_rows_to_csv(&rows)?;
    bulk_insert_clickhouse(&ch_url, &csv_path).await?;
    println!("Inserted into tokens_backfill");

    // 4) Apply updates to main table
    apply_updates(&ch_url).await?;
    println!("Backfill applied.");

    Ok(())
}

/// Pull token_address where any of name/symbol/token_uri is missing (or authorities if you like).
async fn fetch_incomplete_mints(ch_url: &str) -> Result<Vec<String>> {
    // Tweak this WHERE to widen/narrow what “incomplete” means for you.
    let q = r#"
        SELECT token_address
        FROM tokens
        WHERE
            (name = '' OR name IS NULL)
         OR (symbol = '' OR symbol IS NULL)
         OR (token_uri = '' OR token_uri IS NULL)
         OR (update_authority IS NULL OR update_authority = '')
         OR (mint_authority IS NULL OR mint_authority = '')
         OR (freeze_authority IS NULL OR freeze_authority = '')
    "#;

    let client = Client::new();
    let resp = client
        .post(format!("{ch_url}/?query={}", urlencoding::encode(q)))
        .send().await?
        .error_for_status()?
        .text().await?;

    let mints: Vec<String> = resp
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    Ok(mints)
}

/// Load mapping of update_authority -> protocol index.
/// Built-ins + optional CSV file "pubkey,protocol".
fn load_protocol_map(path: Option<&str>) -> HashMap<String, u8> {
    let mut map: HashMap<String, u8> = HashMap::new();

    // ---- Built-in examples (extend with your real values) ----
    // ORE example you gave:
    map.insert("TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM".to_string(), 1);
    map.insert("WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh".to_string(), 2);



    if let Some(p) = path {
        if let Ok(mut rdr) = csv::ReaderBuilder::new().has_headers(true).from_path(p) {
            for rec in rdr.records().flatten() {
                if rec.len() >= 2 {
                    let ua = rec[0].trim().to_string();
                    let prot: u8 = rec[1].trim().parse().unwrap_or(0);
                    if !ua.is_empty() { map.insert(ua, prot); }
                }
            }
        }
    }
    map
}

/// Stream-parse the snapshot tar.* and extract rows for the `wanted` mints.
fn scan_snapshot(snapshot_path: &str, wanted: &HashSet<Pubkey>, ua_map: &HashMap<String, u8>)
-> Result<HashMap<Pubkey, Row>>
{
    let p = Path::new(snapshot_path);
    let file = File::open(p).with_context(|| format!("open {}", snapshot_path))?;

    // Choose decoder by extension
    let reader: Box<dyn Read> = if snapshot_path.ends_with(".tar.zst") {
        Box::new(zstd::stream::read::Decoder::new(BufReader::new(file))?)
    } else if snapshot_path.ends_with(".tar.bz2") || snapshot_path.ends_with(".tbz") {
        Box::new(bzip2::read::BzDecoder::new(BufReader::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    let mut ar = Archive::new(reader);

    let mut rows: HashMap<Pubkey, Row> = HashMap::new();
    // Keep latest write_version per *account key* (avoid older rows from AppendVecs)
    let mut latest_wv: HashMap<Pubkey, u64> = HashMap::new();

    for entry in ar.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().to_string();
        if !path.starts_with("accounts/") { continue; }

        // AppendVec API expects a file path; dump entry to a temp file.
        let tmp = NamedTempFile::new()?;
        std::io::copy(&mut entry, &mut tmp.as_file().try_clone()?)?;
        let tmppath = tmp.into_temp_path();
        let av = AppendVec::new_from_file(tmppath.to_path_buf())?;

        // Iterate stored accounts (bank_id=0 for newest view in snapshots)
        av.accounts(0).for_each(|(meta, acct)| {
            // Dedupe by latest write_version
            if let Some(prev) = latest_wv.get(&meta.pubkey) {
                if *prev > meta.write_version { return; }
            }

            let owner = acct.owner.to_string();

            // --- Metaplex Token Metadata ---
            if owner == METAPLEX_OWNER {
                if let Ok(m) = MplMetadata::deserialize(&mut &acct.data[..]) {
                    if !wanted.contains(&m.mint) { return; }
                    latest_wv.insert(meta.pubkey, meta.write_version);

                    let name   = trim_nul(&m.data.name);
                    let symbol = trim_nul(&m.data.symbol);
                    let uri    = trim_nul(&m.data.uri);
                    let is_mut = if m.is_mutable != 0 { 1 } else { 0 };
                    let ua_str = m.update_authority.to_string();
                    let protocol = *ua_map.get(&ua_str).unwrap_or(&0);

                    rows.entry(m.mint)
                        .and_modify(|r| {
                            if r.name.is_empty()      { r.name = name.clone(); }
                            if r.symbol.is_empty()    { r.symbol = symbol.clone(); }
                            if r.token_uri.is_empty() { r.token_uri = uri.clone(); }
                            r.is_mutable       = is_mut;
                            r.update_authority = Some(ua_str.clone());
                            r.protocol         = protocol;
                        })
                        .or_insert(Row {
                            token_address: m.mint.to_string(),
                            name, symbol, token_uri: uri,
                            is_mutable: is_mut,
                            update_authority: Some(ua_str),
                            mint_authority: None,
                            freeze_authority: None,
                            protocol,
                        });
                }
                return;
            }

            // --- SPL Token Mint (classic) ---
            if owner == TOKEN_OWNER {
                if !wanted.contains(&meta.pubkey) { return; }
                if let Ok(mint) = SplMint::unpack(&acct.data) {
                    latest_wv.insert(meta.pubkey, meta.write_version);
                    let ma = mint.mint_authority.map(|p| p.to_string());
                    let fa = mint.freeze_authority.map(|p| p.to_string());

                    rows.entry(meta.pubkey)
                        .and_modify(|r| {
                            if r.mint_authority.is_none()  { r.mint_authority  = ma.clone(); }
                            if r.freeze_authority.is_none(){ r.freeze_authority= fa.clone(); }
                        })
                        .or_insert(Row{
                            token_address: meta.pubkey.to_string(),
                            name: String::new(), symbol: String::new(), token_uri: String::new(),
                            is_mutable: 0, update_authority: None,
                            mint_authority: ma, freeze_authority: fa,
                            protocol: 0,
                        });
                }
                return;
            }
        });
    }
    Ok(rows)
}

fn trim_nul(s: &str) -> String {
    s.trim_end_matches('\0').to_string()
}

fn write_rows_to_csv(rows: &HashMap<Pubkey, Row>) -> Result<String> {
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.into_temp_path().to_string_lossy().to_string();

    let mut w = WriterBuilder::new().has_headers(true).from_path(&path)?;
    w.write_record(&[
        "token_address","name","symbol","token_uri","is_mutable",
        "update_authority","mint_authority","freeze_authority","protocol"
    ])?;
    for (_k, r) in rows {
        w.write_record(&[
            r.token_address.as_str(),
            r.name.as_str(),
            r.symbol.as_str(),
            r.token_uri.as_str(),
            &r.is_mutable.to_string(),
            r.update_authority.clone().unwrap_or_default().as_str(),
            r.mint_authority.clone().unwrap_or_default().as_str(),
            r.freeze_authority.clone().unwrap_or_default().as_str(),
            &r.protocol.to_string(),
        ])?;
    }
    w.flush()?;
    Ok(path)
}

async fn bulk_insert_clickhouse(ch_url: &str, csv_path: &str) -> Result<()> {
    let client = Client::new();
    let sql = "INSERT INTO tokens_backfill FORMAT CSVWithNames";
    let url = format!("{}/?query={}", ch_url, urlencoding::encode(sql));
    let bytes = tokio::fs::read(csv_path).await?;
    client.post(url)
        .header("Content-Type", "text/plain")
        .body(bytes)
        .send().await?
        .error_for_status()?;
    Ok(())
}

async fn apply_updates(ch_url: &str) -> Result<()> {
    let client = Client::new();
    let update_sql = r#"
        ALTER TABLE tokens
        UPDATE
            name             = IF((name = '' OR name IS NULL) AND b.name != '', b.name, name),
            symbol           = IF((symbol = '' OR symbol IS NULL) AND b.symbol != '', b.symbol, symbol),
            token_uri        = IF((token_uri = '' OR token_uri IS NULL) AND b.token_uri != '', b.token_uri, token_uri),
            is_mutable       = IF(is_mutable = 0, b.is_mutable, is_mutable),
            update_authority = IF((update_authority IS NULL OR update_authority = ''), b.update_authority, update_authority),
            mint_authority   = IF((mint_authority   IS NULL OR mint_authority   = ''), b.mint_authority,   mint_authority),
            freeze_authority = IF((freeze_authority IS NULL OR freeze_authority = ''), b.freeze_authority, freeze_authority),
            protocol         = IF(protocol = 0, b.protocol, protocol)
        WHERE token_address IN (SELECT token_address FROM tokens_backfill AS b)
        SETTINGS mutations_sync = 1
    "#;
    let url = format!("{}/?query={}", ch_url, urlencoding::encode(update_sql));
    client.post(url).send().await?.error_for_status()?;
    Ok(())
}
