import os
import requests
import json
import time
import sys

# --- Configuration ---
# Your custom RPC endpoint with your API key.
RPC_URL = "https://mainnet.helius-rpc.com/?api-key=aff4afad-2197-4d03-afdd-a77bf24ce258"
# Base URL for the Old Faithful epoch files.
CAR_URL_TEMPLATE = "https://files.old-faithful.net/{}/epoch-{}.car"
# Directory where the file will be saved.
DOWNLOAD_DIR = "solana_epochs"

def get_latest_epoch():
    """
    Queries a Solana RPC endpoint to get the current epoch number.
    """
    headers = {"Content-Type": "application/json"}
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo"
    }

    try:
        response = requests.post(RPC_URL, headers=headers, data=json.dumps(body))
        response.raise_for_status()
        
        result = response.json()
        if 'result' in result and 'epoch' in result['result']:
            epoch_number = result['result']['epoch']
            print(f"✅ Found latest epoch number: {epoch_number}")
            return epoch_number
        else:
            print("❌ ERROR: Could not find epoch number in RPC response.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR: Failed to get epoch info from RPC: {e}")
        return None

def download_epoch_with_progress(epoch_number):
    """
    Downloads a single epoch CAR file with a progress bar.
    """
    if epoch_number is None:
        print("Cannot download, epoch number is not available.")
        return

    # Subtract 2 from the latest epoch to target an older, more likely to be available, epoch.
    target_epoch = epoch_number - 2
    
    url = CAR_URL_TEMPLATE.format(target_epoch, target_epoch)
    print("LINK", url)
    output_path = os.path.join(DOWNLOAD_DIR, f"epoch-{target_epoch}.car")

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    print(f"Starting download for epoch {target_epoch}...")
    
    try:
        with requests.get(url, stream=True, timeout=600) as response:
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            if total_size > 0:
                print(f"Total file size: {total_size / (1024 * 1024 * 1024):.2f} GB")
            
            downloaded_bytes = 0
            start_time = time.time()
            
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    downloaded_bytes += len(chunk)
                    file.write(chunk)
                    
                    # Update progress bar
                    progress = int(50 * downloaded_bytes / total_size)
                    speed = (downloaded_bytes / (time.time() - start_time)) / 1024 / 1024 # MB/s
                    
                    # Estimate remaining time
                    if speed > 0:
                        remaining_time_sec = (total_size - downloaded_bytes) / (speed * 1024 * 1024)
                        remaining_time_str = time.strftime("%H:%M:%S", time.gmtime(remaining_time_sec))
                    else:
                        remaining_time_str = "N/A"

                    sys.stdout.write("\r[{}{}] {:>6.2f} MB/s | {}% | ETA: {}".format(
                        '█' * progress, '.' * (50 - progress), speed,
                        int(downloaded_bytes / total_size * 100), remaining_time_str))
                    sys.stdout.flush()

        sys.stdout.write('\n')
        print(f"✅ Download of epoch {target_epoch} completed successfully!")
        print(f"File saved to: {output_path}")

    except requests.exceptions.HTTPError as err:
        print(f"❌ ERROR: Failed to download epoch {target_epoch}. Status code: {err.response.status_code}")
        if err.response.status_code == 404:
            print("The file was not found. Try again with an even older epoch.")
    except requests.exceptions.RequestException as err:
        print(f"❌ ERROR: An error occurred during the request for epoch {target_epoch}: {err}")
    except Exception as e:
        print(f"❌ ERROR: An unexpected error occurred: {e}")

if __name__ == "__main__":
    latest_epoch_number = get_latest_epoch()
    download_epoch_with_progress(latest_epoch_number)