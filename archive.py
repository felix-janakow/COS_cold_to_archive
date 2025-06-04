import time
from ibm_boto3 import client
from ibm_botocore.client import Config
import os
from dotenv import load_dotenv
import logging
from tqdm import tqdm
import glob
from datetime import timedelta

# --- Control Plane: All configurable parameters in one place ---

MAX_KEYS_PER_FILE = 250            # Max lines per key file before rotating
BATCH_SIZE = 100                   # Number of objects per batch
MAX_RETRIES = 3                    # Max retries for copy operations
BACKOFF_FACTOR = 2                 # Exponential backoff factor
THROTTLE_DELAY = 0.1               # Delay (in seconds) between API calls
USE_EMOJIS = True                  # Emoji output in logs

COPIED_KEYS_DIR = "copied_keys"
FAILED_KEYS_DIR = "failed_keys"
LOG_DIR = "logs"

os.makedirs(COPIED_KEYS_DIR, exist_ok=True)
os.makedirs(FAILED_KEYS_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

COPIED_KEYS_PREFIX = os.path.join(COPIED_KEYS_DIR, "copied_keys")
FAILED_KEYS_PREFIX = os.path.join(FAILED_KEYS_DIR, "failed_keys")
LOG_FILE = os.path.join(LOG_DIR, "cos_batch_copy.log")
ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), ".env")

# --- Input Handling ---

def collect_user_input():
    """Asks the user for input and saves it to the .env file. Keeps previous values if input is empty, except for prefix."""
    # Load existing values from .env file if it exists
    old_values = {}
    if os.path.exists(ENV_FILE_PATH):
        with open(ENV_FILE_PATH, "r") as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    old_values[k] = v


# --- Input Console ---

    print("Please enter the following details (leave empty to keep previous value, except for prefix):")

    bucket = input(f"BUCKET (used for both source and destination) [{old_values.get('SOURCE_BUCKET', '')}]: ") or old_values.get('SOURCE_BUCKET', '')

    iam_api_key = input(f"IAM_API_KEY [{old_values.get('IAM_API_KEY', '')}]: ") or old_values.get('IAM_API_KEY', '')

    region = input(f"REGION [{old_values.get('REGION', '')}]: ") or old_values.get('REGION', '')

    keyprotect_crn = input(f"KEY_PROTECT_CRN (leave empty if not used) [{old_values.get('KEY_PROTECT_CRN', '')}]: ") or old_values.get('KEY_PROTECT_CRN', '')

    prefix = input(f"OPTIONAL: Folder/Path within the bucket (e.g. 'folder1/' or leave empty to archive everything) [now on: {old_values.get('OBJECT_PREFIX', '')}]: ").strip()
    # Safes values only if something is entered, otherwise keeps the old value


    # Save the input from the Input Console to the .env file
    with open(ENV_FILE_PATH, "w") as env_file:
        env_file.write(f"SOURCE_BUCKET={bucket}\n")
        env_file.write(f"DESTINATION_BUCKET={bucket}\n")
        env_file.write(f"IAM_API_KEY={iam_api_key}\n")
        env_file.write(f"REGION={region}\n")
        if keyprotect_crn:
            env_file.write(f"KEY_PROTECT_CRN={keyprotect_crn}\n")
        if prefix != "":
            env_file.write(f"OBJECT_PREFIX={prefix}\n")
        # Wenn prefix leer, wird keine OBJECT_PREFIX-Zeile geschrieben (also alles im Bucket verarbeitet)

    print("\nConfiguration has been saved to the .env file.")

def ensure_env():
    """Ensures that the .env file exists and loads it."""
    if os.path.exists(ENV_FILE_PATH):
        print("A .env file was found.")
        choice = input("Do you want to enter new values? (y/N): ").strip().lower()
        if choice == "y":
            collect_user_input()
        else:
            print("Using existing values from the .env file.")
    else:
        print("No .env file found. New input required.")
        collect_user_input()
    load_dotenv(ENV_FILE_PATH)

# --- COS Copy Functions ---

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Emoji support function
def get_icon(emoji, fallback):
    """Returns an emoji or a fallback text."""
    return emoji if USE_EMOJIS else fallback

# Emojis with fallback texts
CHECK_ICON = get_icon("✅", "[SUCCESS]")
ERROR_ICON = get_icon("❌", "[ERROR]")
RETRY_ICON = get_icon("♻️", "[RETRY]")
INFO_ICON = get_icon("🔄", "[INFO]")
MAIL_ICON = get_icon("📭", "[NO FAILED KEYS]")

def retry_with_backoff(func, max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR):
    """Executes a function with exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                raise
            time.sleep(backoff_factor ** attempt)

def throttle(api_call, delay=THROTTLE_DELAY):
    """Throttles API calls."""
    time.sleep(delay)
    return api_call()

# --- Key handling with file rotation ---

def load_all_keys(prefix):
    files = sorted(glob.glob(f"{prefix}_*.txt"))
    keys = set()
    for fname in files:
        with open(fname, "r") as f:
            keys.update(line.strip() for line in f)
    return keys

def get_current_file(prefix):
    files = sorted(glob.glob(f"{prefix}_*.txt"))
    if not files:
        return f"{prefix}_1.txt"
    return files[-1]

def save_key(key, prefix):
    fname = get_current_file(prefix)
    # Check line count
    if os.path.exists(fname):
        with open(fname, "r") as f:
            lines = sum(1 for _ in f)
    else:
        lines = 0
    if lines >= MAX_KEYS_PER_FILE:
        idx = int(fname.split('_')[-1].split('.')[0]) + 1
        fname = f"{prefix}_{idx}.txt"
    with open(fname, "a") as f:
        f.write(f"{key}\n")

def load_copied_keys():
    return load_all_keys(COPIED_KEYS_PREFIX)

def save_copied_key(key):
    save_key(key, COPIED_KEYS_PREFIX)

def load_failed_keys():
    return list(load_all_keys(FAILED_KEYS_PREFIX))

def save_failed_key(key):
    save_key(key, FAILED_KEYS_PREFIX)

def clear_failed_keys():
    for fname in glob.glob(f"{FAILED_KEYS_PREFIX}_*.txt"):
        open(fname, "w").close()

def count_total_keys(s3, bucket, prefix=""):
    total = 0
    paginator = s3.get_paginator('list_objects_v2')
    paginate_kwargs = {"Bucket": bucket}
    if prefix:
        paginate_kwargs["Prefix"] = prefix
    for page in paginator.paginate(**paginate_kwargs):
        total += len(page.get('Contents', []))
    return total

def process_batch(s3, source_bucket, destination_bucket, batch, batch_number, copied_keys, max_retries=MAX_RETRIES):
    successful_copies = 0
    keyprotect_crn = os.environ.get("KEY_PROTECT_CRN")
    for key in batch:
        if key in copied_keys:
            continue

        copy_source = {
            'Bucket': source_bucket,
            'Key': key
        }

        def copy_object():
            kwargs = dict(
                CopySource=copy_source,
                Bucket=destination_bucket,
                Key=key,
                MetadataDirective="REPLACE"
            )
            if keyprotect_crn:
                kwargs["ServerSideEncryption"] = "ibm-kms"
                kwargs["SSEKMSKeyId"] = keyprotect_crn
            s3.copy_object(**kwargs)

        success = False
        for attempt in range(1, max_retries + 1):
            try:
                retry_with_backoff(lambda: throttle(copy_object))
                tqdm.write(f"{CHECK_ICON} [{batch_number}] {key} moved to archive successfully (attempt {attempt}).")
                logging.info(f"[{batch_number}] {key} moved to archive successfully (attempt {attempt})")
                save_copied_key(key)
                remove_key_from_failed_keys(key)  
                successful_copies += 1
                success = True
                break
            except Exception as e:
                error_message = str(e)
                if "InvalidObjectState" in error_message and "Operation is not valid for the source object's storage class" in error_message:
                    tqdm.write(f"{CHECK_ICON} [{batch_number}] {key} already archived or in archive tier (treated as success).")
                    logging.info(f"[{batch_number}] {key} already archived or in archive tier (treated as success).")
                    save_copied_key(key)
                    remove_key_from_failed_keys(key)
                    successful_copies += 1
                    success = True
                    break
                else:
                    tqdm.write(f"{ERROR_ICON} [{batch_number}] Error moving {key} to archive (attempt {attempt}): {e}")
                    logging.warning(f"[{batch_number}] Error moving {key} to archive (attempt {attempt}): {e}")

        if not success:
            save_failed_key(key)

    return successful_copies

def copy_objects_in_batches(source_bucket, destination_bucket, batch_size=BATCH_SIZE):
    s3 = client(
        's3',
        ibm_api_key_id=os.environ['IAM_API_KEY'],
        config=Config(signature_version='oauth'),
        endpoint_url=f"https://s3.{os.environ['REGION']}.cloud-object-storage.appdomain.cloud"
    )

    copied_keys = load_copied_keys()
    prefix = os.environ.get("OBJECT_PREFIX", "").strip()
    total_keys = count_total_keys(s3, source_bucket, prefix=prefix)
    total_to_process = total_keys - len(copied_keys)

    if total_to_process <= 0:
        tqdm.write(f"{CHECK_ICON} All files have already been processed.")
        return

    tqdm.write(f"{INFO_ICON} Total files to process: {total_to_process}")

    paginator = s3.get_paginator('list_objects_v2')
    batch = []
    batch_number = 1
    processed = 0
    failed = 0

    paginate_kwargs = {"Bucket": source_bucket}
    if prefix:
        paginate_kwargs["Prefix"] = prefix

    def format_eta(seconds):
        """Format seconds as hh:mm:ss."""
        return str(timedelta(seconds=int(seconds)))

    with tqdm(
        total=total_to_process,
        desc=f"{INFO_ICON} Processing",
        unit="obj",
        dynamic_ncols=True,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}, ETA: {postfix}]"
    ) as pbar:
        for page in paginator.paginate(**paginate_kwargs):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key in copied_keys:
                    continue

                batch.append(key)

                if len(batch) >= batch_size:
                    tqdm.write(f"{INFO_ICON} Processing batch {batch_number} with {len(batch)} objects...")
                    successful = process_batch(s3, source_bucket, destination_bucket, batch, batch_number, copied_keys)
                    processed += successful
                    failed += (batch_size - successful)
                    batch = []
                    batch_number += 1
                    # ETA calculation
                    if pbar.n > 0:
                        rate = pbar.n / pbar.format_dict['elapsed']
                        remaining = (pbar.total - pbar.n) / rate if rate > 0 else 0
                        pbar.set_postfix_str(format_eta(remaining))
                    pbar.update(successful)

        # Process the last batch
        if batch:
            tqdm.write(f"{INFO_ICON} Processing last batch {batch_number} with {len(batch)} objects...")
            successful = process_batch(s3, source_bucket, destination_bucket, batch, batch_number, copied_keys)
            processed += successful
            failed += (len(batch) - successful)
            if pbar.n > 0:
                rate = pbar.n / pbar.format_dict['elapsed']
                remaining = (pbar.total - pbar.n) / rate if rate > 0 else 0
                pbar.set_postfix_str(format_eta(remaining))
            pbar.update(successful)

    tqdm.write(f"{CHECK_ICON} Processing complete. Successfully processed: {processed} of {total_to_process}")
    tqdm.write(f"{ERROR_ICON} {failed} of {total_to_process} objects failed")
    logging.info(f"Processing complete. Successfully processed: {processed} of {total_to_process}")
    logging.info(f"Failed objects: {failed} of {total_to_process}")

    # Show absolute stats: all copied keys vs. all keys in bucket
    copied_keys_total = len(load_copied_keys())
    tqdm.write(f"{CHECK_ICON} Total archived keys: {copied_keys_total} of {total_keys} in bucket.")

def retry_failed_keys(source_bucket, destination_bucket, max_retries=MAX_RETRIES):
    s3 = client(
        's3',
        ibm_api_key_id=os.environ['IAM_API_KEY'],
        config=Config(signature_version='oauth'),
        endpoint_url=f"https://s3.{os.environ['REGION']}.cloud-object-storage.appdomain.cloud"
    )

    keyprotect_crn = os.environ.get("KEY_PROTECT_CRN")  #

    prefix = os.environ.get("OBJECT_PREFIX", "").strip()
    failed_keys = load_failed_keys()
    if prefix:
        failed_keys = [k for k in failed_keys if k.startswith(prefix)]
    if not failed_keys:
        tqdm.write(f"{MAIL_ICON} No failed keys present.")
        return

    copied_keys = load_copied_keys()
    remaining_keys = []
    total = len(failed_keys)

    tqdm.write(f"{RETRY_ICON} Retrying {total} failed objects...")

    with tqdm(total=total, desc=f"{RETRY_ICON} Retry", unit="obj") as pbar:
        for key in failed_keys:
            if key in copied_keys:
                pbar.update(1)
                continue

            copy_source = {
                'Bucket': source_bucket,
                'Key': key
            }

            success = False
            for attempt in range(1, max_retries + 1):
                try:
                    kwargs = dict(
                        CopySource=copy_source,
                        Bucket=destination_bucket,
                        Key=key,
                        MetadataDirective="REPLACE"
                    )
                    if keyprotect_crn:
                        kwargs["ServerSideEncryption"] = "ibm-kms"
                        kwargs["SSEKMSKeyId"] = keyprotect_crn
                    s3.copy_object(**kwargs)
                    tqdm.write(f"{CHECK_ICON} RETRY: {key} moved to archive successfully (attempt {attempt})")
                    logging.info(f"RETRY: {key} moved to archive successfully (attempt {attempt})")
                    save_copied_key(key)
                    remove_key_from_failed_keys(key)  
                    success = True
                    break
                except Exception as e:
                    error_message = str(e)
                    if "InvalidObjectState" in error_message and "Operation is not valid for the source object's storage class" in error_message:
                        tqdm.write(f"{CHECK_ICON} RETRY: {key} already archived or in archive tier.")
                        logging.info(f"RETRY: {key} already archived or in archive tier (treated as success).")
                        save_copied_key(key)
                        remove_key_from_failed_keys(key)
                        success = True
                        break
                    else:
                        tqdm.write(f"{ERROR_ICON} RETRY error moving {key} to archive (attempt {attempt}): {e}")
                        logging.warning(f"RETRY: Error moving {key} to archive (attempt {attempt}): {e}")

            if not success:
                remaining_keys.append(key)

            pbar.update(1)

    # Write remaining failed keys to new file(s)
    # We also rotate here if > MAX_KEYS_PER_FILE
    idx = 1
    written = 0
    if remaining_keys:
        for i in range(0, len(remaining_keys), MAX_KEYS_PER_FILE):
            fname = f"{FAILED_KEYS_PREFIX}_{idx}.txt"
            with open(fname, "w") as f:
                for key in remaining_keys[i:i+MAX_KEYS_PER_FILE]:
                    f.write(f"{key}\n")
            idx += 1
            written += len(remaining_keys[i:i+MAX_KEYS_PER_FILE])
    else:
        clear_failed_keys()

    tqdm.write(f"{RETRY_ICON} Retry complete. Still remaining: {len(remaining_keys)}")
    logging.info(f"Retry complete. Remaining errors: {len(remaining_keys)}")



# Remove successfully copied keys from failed_keys files
def remove_key_from_failed_keys(key):
    """Remove a key from all failed_keys files if it was successfully archived."""
    for fname in glob.glob(f"{FAILED_KEYS_PREFIX}_*.txt"):
        if not os.path.exists(fname):
            continue
        with open(fname, "r") as f:
            lines = f.readlines()
        new_lines = [line for line in lines if line.strip() != key]
        if len(new_lines) != len(lines):
            with open(fname, "w") as f:
                f.writelines(new_lines)



# --- Main Execution ---
if __name__ == '__main__':
    ensure_env()
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    mode = os.environ.get("COPY_MODE", "normal")

    if mode == "retry":
        retry_failed_keys(source_bucket, destination_bucket)
    else:
        copy_objects_in_batches(source_bucket, destination_bucket, batch_size=BATCH_SIZE)
