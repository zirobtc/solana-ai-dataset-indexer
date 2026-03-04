#!/usr/bin/env bash
apt-get update
apt-get install -y curl build-essential pkg-config libssl-dev ca-certificates
curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal
. $HOME/.cargo/env
cargo --version
rustc --version


cypher-shell -u neo4j -p neo4j123 \
  "CREATE CONSTRAINT wallet_address IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE;"

cypher-shell -u neo4j -p neo4j123 \
  "CREATE CONSTRAINT token_address IF NOT EXISTS FOR (t:Token) REQUIRE t.address IS UNIQUE;"

#   clickhouse-client \
#     --query "INSERT INTO known_wallets FORMAT TabSeparatedWithNames" \
#     < known_wallets.tsv

# hf download zirobtc/pump-fun-dataset \
#   token_metrics_latest_epoch_851.parquet \
#   wallet_profiles_epoch_851.parquet \
#   wallet_profile_metrics_latest_epoch_851.parquet \
#   wallet_holdings_latest_epoch_851.parquet \
#   --local-dir .