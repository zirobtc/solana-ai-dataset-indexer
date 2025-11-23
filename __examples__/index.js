// --- Hardcoded Arguments (no need for argparse!)
const RPC_ADDRESS = "https://api.mainnet-beta.solana.com";
const MAX_SNAPSHOT_AGE_IN_SLOTS = 1300; // ~30 days
const THREADS_COUNT = 1000;

// --- Imports
const { Connection } = require("@solana/web3.js");
const https = require("https");
const url = require("url");

// --- Main Function
async function findSnapshots() {
  try {
    const connection = new Connection(RPC_ADDRESS);

    // 1. Get the current slot
    const currentSlot = await connection.getSlot();
    console.log(`Current Slot: ${currentSlot}`);

    // 2. Get a list of all cluster nodes
    const clusterNodes = await connection.getClusterNodes();

    const rpcNodes = clusterNodes
      .filter((node) => node.rpc) // Only get nodes with a public RPC address
      .map(node => new URL(`http://${node.rpc}`).hostname); 

    console.log(`Found ${rpcNodes.length} RPC nodes to check.`);

    // 3. Concurrently check each RPC for a suitable snapshot
    const promises = rpcNodes.map((rpcHostname) =>
      checkRpcForSnapshot(rpcHostname, currentSlot)
    );

    const results = await Promise.allSettled(promises);

    const suitableNodes = results
      .filter((result) => result.status === "fulfilled" && result.value)
      .map((result) => result.value);

    console.log("--- Suitable Snapshots ---");
    console.log(JSON.stringify(suitableNodes, null, 2));
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

// --- Helper Function to Check a Single RPC Node
async function checkRpcForSnapshot(rpcHostname, currentSlot) {
  const rpcUrl = "https://api.mainnet-beta.solana.com";

  // The RPC method and parameters to request the highest snapshot slot
  const requestBody = {
    jsonrpc: "2.0",
    id: 1,
    method: "getHighestSnapshotSlot",
  };

  fetch(rpcUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(requestBody),
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.result) {
        const { full, incremental } = data.result;
        console.log("Found latest snapshots:");
        console.log("Full Snapshot Slot:", full[0]);
        console.log("Incremental Snapshot Slot:", incremental[0]);

        // Construct the download URL for the full snapshot
        const downloadUrl = `https://api.mainnet-beta.solana.com/snapshot.tar.zst`;
        console.log("Download URL:", downloadUrl);
      } else {
        console.error("Could not retrieve snapshot information:", data.error);
      }
    })
    .catch((error) => {
      console.error("An error occurred while fetching snapshot info:", error);
    });

}

findSnapshots();
