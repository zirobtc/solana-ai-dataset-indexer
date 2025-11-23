use anyhow::{Error, Result};
use clickhouse::{Client, Row};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub async fn insert_rows<T: clickhouse::Row + Send + Serialize>(
    db_client: &Client,
    table_name: &str,
    rows: Vec<T>,
    handler_name: &str,
    action_type: &str,
) -> Result<(), Error> {
    if !rows.is_empty() {
        let count = rows.len();
        let mut inserter = db_client.insert(table_name)?;
        for row in rows {
            inserter.write(&row).await?;
        }
        inserter.end().await?;
        println!(
            "[db] Stored {} {} {} actions.",
            count, handler_name, action_type
        );
    }
    Ok(())
}
