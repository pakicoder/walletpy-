import time
import os
import threading
import secrets
import hashlib
import ecdsa
import tarfile
import requests
from datetime import datetime, timedelta

# Pinata JWT key as variable
MY_JWT = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySW5mb3JtYXRpb24iOnsiaWQiOiIwNTNiNTk1YS01N2ZjLTQ2ZWItOWVkNy0yZGU5ZDU3MTJmNTEiLCJlbWFpbCI6ImNrdWx0aGU1NkBnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicGluX3BvbGljeSI6eyJyZWdpb25zIjpbeyJkZXNpcmVkUmVwbGljYXRpb25Db3VudCI6MSwiaWQiOiJGUkExIn0seyJkZXNpcmVkUmVwbGljYXRpb25Db3VudCI6MSwiaWQiOiJOWUMxIn1dLCJ2ZXJzaW9uIjoxfSwibWZhX2VuYWJsZWQiOmZhbHNlLCJzdGF0dXMiOiJBQ1RJVkUifSwiYXV0aGVudGljYXRpb25UeXBlIjoic2NvcGVkS2V5Iiwic2NvcGVkS2V5S2V5IjoiZmZmNjQwOWRlNTg5ZmU1YTdlZjQiLCJzY29wZWRLZXlTZWNyZXQiOiIwNGVjYTY1OTllZjFiZTRlODc0YmU0YmY3NzkzMzEyNjZmNGY2NGIwZTllMGQyYmE4YTk4MzFhZTBlNzBlNmMzIiwiZXhwIjoxNzkxMjc2NzA5fQ.LMj1DSwxbXpreTy2pXEKcBpkXck3LY0VSk6TjfCZ5r8'

def generate_ethereum_wallet():
    privkey = secrets.token_bytes(32)
    sk = ecdsa.SigningKey.from_string(privkey, curve=ecdsa.SECP256k1)
    vk = sk.verifying_key
    pubkey = vk.to_string()
    keccak = hashlib.sha3_256(pubkey).digest()
    address = '0x' + keccak[-20:].hex()
    pk_hex = '0x' + privkey.hex()
    return f"{address}:{pk_hex}"

def worker(entries, entry_lock, stop_time):
    while time.time() < stop_time:
        entry = generate_ethereum_wallet()
        with entry_lock:
            entries.append(entry)

def write_to_file(entries, filename, file_lock):
    with file_lock:
        mode = 'a' if os.path.exists(filename) else 'w'
        with open(filename, mode) as f:
            if mode == 'a' and os.stat(filename).st_size > 0:
                f.write(f", {','.join(entries)}")
            else:
                f.write(','.join(entries))

def encode_and_upload_to_ipfs(filename, block_number, date_log, hour, half, file_lock, delete_local=True):
    with file_lock:
        try:
            # Create .tar.gz archive
            tar_filename = f"{filename[:-4]}.tar.gz"  # Remove .txt, add .tar.gz
            with tarfile.open(tar_filename, "w:gz") as tar:
                tar.add(filename, arcname=os.path.basename(filename))
            
            # Upload to Pinata IPFS
            url = "https://api.pinata.cloud/pinning/pinFileToIPFS"
            headers = {"Authorization": f"Bearer {MY_JWT}"}
            with open(tar_filename, 'rb') as f:
                files = {'file': (os.path.basename(tar_filename), f)}
                response = requests.post(url, headers=headers, files=files)
            
            if response.status_code == 200:
                result = response.json()
                cid = result['IpfsHash']
                with open('filelist.txt', 'a') as fl:
                    fl.write(f"{date_log}, Hour {hour}, {half.capitalize()} Half: {cid}\n")
                print(f"Uploaded {date_log}, Hour {hour}, {half.capitalize()} Half to IPFS: ipfs://{cid}")
            else:
                print(f"Pinata API error for {tar_filename}: {response.status_code} - {response.text}")
                return None
            
            # Cleanup
            os.remove(tar_filename)
            if delete_local:
                os.remove(filename)
                print(f"Deleted local {filename}")
            return cid
        except Exception as e:
            print(f"Upload failed for {filename}: {e}")
            return None

def get_initial_start_time(metadata_file="wallets.txt"):
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            first_line = f.readline().strip()
            try:
                return float(first_line)
            except ValueError:
                return time.time()
    else:
        start_time = time.time()
        with open(metadata_file, 'w') as f:
            f.write(str(start_time) + '\n')
        return start_time

def main():
    base_dir = "./mywalletsFolder"
    os.makedirs(base_dir, exist_ok=True)
    max_workers = 10
    file_lock = threading.Lock()
    initial_start_time = get_initial_start_time()
    cycle = 0
    minutes_per_block = 30
    seconds_per_block = minutes_per_block * 60
    last_block_uploaded = 0
    delete_local = True

    if not os.path.exists('filelist.txt'):
        with open('filelist.txt', 'w') as f:
            f.write("IPFS Wallet File List\n")

    while time.time() - initial_start_time < 2592000:  # 30 days
        cycle += 1
        elapsed_time = time.time() - initial_start_time
        current_block = int(elapsed_time // seconds_per_block) + 1
        current_time = datetime.fromtimestamp(initial_start_time + elapsed_time)
        date_str = current_time.strftime("%Y%m%d")  # YYYYMMDD for filename
        date_log = current_time.strftime("%Y-%m-%d")  # YYYY-MM-DD for filelist
        hour = current_time.strftime("%I").lstrip("0") or "12"  # 1-12 format
        am_pm = current_time.strftime("%p").lower()  # am/pm
        half = "first" if current_block % 2 == 1 else "second"  # First or second half-hour
        hour_number = int(current_time.strftime("%H")) + 1  # For logging (1-24)
        filename = os.path.join(base_dir, f"wallets_{date_str}_{hour}{am_pm}_{half}.txt")
        
        # Upload previous block's file if a new block has started
        if current_block > last_block_uploaded + 1:
            prev_half = "first" if (last_block_uploaded + 1) % 2 == 1 else "second"
            prev_hour_time = datetime.fromtimestamp(initial_start_time + (last_block_uploaded * seconds_per_block))
            prev_date_str = prev_hour_time.strftime("%Y%m%d")
            prev_date_log = prev_hour_time.strftime("%Y-%m-%d")
            prev_hour = prev_hour_time.strftime("%I").lstrip("0") or "12"
            prev_am_pm = prev_hour_time.strftime("%p").lower()
            prev_hour_number = int(prev_hour_time.strftime("%H")) + 1
            prev_filename = os.path.join(base_dir, f"wallets_{prev_date_str}_{prev_hour}{prev_am_pm}_{prev_half}.txt")
            if os.path.exists(prev_filename):
                encode_and_upload_to_ipfs(prev_filename, last_block_uploaded + 1, prev_date_log, prev_hour_number, prev_half, file_lock, delete_local)
                print(f"Pausing for 2 minutes after {prev_date_log}, Hour {prev_hour_number}, {prev_half.capitalize()} Half")
                time.sleep(120)  # 2-minute pause after block upload
            last_block_uploaded = current_block - 1
        
        print(f"Cycle {cycle} ({date_log}, Hour {hour_number}, {half.capitalize()} Half): Generating...")
        gen_start = time.time()
        stop_time = gen_start + 60
        entries = []
        entry_lock = threading.Lock()
        threads = []
        for _ in range(max_workers):
            t = threading.Thread(target=worker, args=(entries, entry_lock, stop_time))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        num_generated = len(entries)
        write_to_file(entries, filename, file_lock)
        print(f"Cycle {cycle}: Generated {num_generated} wallets in {time.time() - gen_start:.2f}s")
        
        # Check if 30 days are complete
        if time.time() - initial_start_time >= 2592000:
            encode_and_upload_to_ipfs(filename, current_block, date_log, hour_number, half, file_lock, delete_local)
            print(f"Pausing for 2 minutes after final {date_log}, Hour {hour_number}, {half.capitalize()} Half")
            time.sleep(120)  # 2-minute pause after final upload
            break
    
    print(f"Done! Check filelist.txt for CIDs. Total cycles: {cycle}")

if __name__ == "__main__":
    main()
