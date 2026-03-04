#!/usr/bin/env bash
apt-get update
apt-get install -y curl build-essential pkg-config libssl-dev ca-certificates
curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal
. $HOME/.cargo/env
cargo --version
rustc --version

