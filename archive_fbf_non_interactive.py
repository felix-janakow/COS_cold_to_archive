import os
import logging
import sqlite3
import threading
import time
from dotenv import load_dotenv
from ibm_boto3 import client
from ibm_botocore.client import Config

# --- Configuration Section ---

BATCH_SIZE = 1000                  # Number of objects per batch
MAX_RETRIES = 3                    # Max retries for copy operations
BACKOFF_FACTOR = 2                 # Exponential backoff factor
THROTTLE_DELAY = 0.3               # Initial delay (in seconds) between API calls
USE_EMOJIS = True                  # Emoji output in logs

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "cos_batch_copy.log")
ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), ".env")
SQLITE_DB = "cos_status.db"
FOLDER_PROGRESS_FILE = "folder_progress.log"

# --- Logging Setup ---
# Only WARNING and ERROR messages will be logged.
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Emoji Support ---

def get_icon(emoji, fallback):
    """Returns an emoji or a fallback text depending on USE_EMOJIS."""
    return emoji if USE_EMOJIS else fallback

CHECK_ICON = get_icon("✅", "[SUCCESS]")
ERROR_ICON = get_icon("❌", "[ERROR]")
RETRY_ICON = get_icon("♻️", "[RETRY]")
INFO_ICON = get_icon("🔄", "[INFO]")
MAIL_ICON = get_icon("📭", "[NO FAILED KEYS]")

# --- Environment Loader ---

def ensure_env():
    """Loads the .env file without any user interaction."""
    if not os.path.exists(ENV_FILE_PATH):
        raise FileNotFoundError(f"No .env file found at {ENV_FILE_PATH}. Please provide one before running.")
    load_dotenv(ENV_FILE_PATH)

# --- Globale SQLite-Verbindung ---
DB_CONN = None

def get_db_conn():
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = sqlite3.connect(SQLITE_DB, check_same_thread=False)
    return DB_CONN

def close_db_conn():
    global DB_CONN
    if DB_CONN:
        DB_CONN.close()
        DB_CONN = None

# --- SQLite Database Functions (angepasst) ---

def init_db():
    """Initializes the SQLite database and creates tables if they do not exist."""
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS copied_keys (
            key TEXT PRIMARY KEY
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS failed_keys (
            key TEXT PRIMARY KEY
        )
    """)
    conn.commit()
    return conn

def save_copied_key_db(key):
    """Saves a copied key to the database."""
    conn = get_db_conn()
    try:
        conn.execute("INSERT OR IGNORE INTO copied_keys (key) VALUES (?)", (key,))
        conn.commit()
    except Exception:
        pass

def save_failed_key_db(key):
    """Saves a failed key to the database."""
    conn = get_db_conn()
    try:
        conn.execute("INSERT OR IGNORE INTO failed_keys (key) VALUES (?)", (key,))
        conn.commit()
    except Exception:
        pass

def remove_key_from_failed_keys_db(key):
    """Removes a key from the failed_keys table."""
    conn = get_db_conn()
    conn.execute("DELETE FROM failed_keys WHERE key = ?", (key,))
    conn.commit()

def is_key_copied_db(key):
    """Checks if a key is already marked as copied in the database."""
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM copied_keys WHERE key = ?", (key,))
    return c.fetchone() is not None

def count_archived_for_prefix_db(prefix):
    """Counts how many keys have been archived for a given prefix."""
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM copied_keys WHERE key LIKE ?", (f"{prefix}%",))
    return c.fetchone()[0]

def get_failed_keys_db(prefix=None):
    """Returns a list of failed keys, optionally filtered by prefix."""
    conn = get_db_conn()
    c = conn.cursor()
    if prefix:
        c.execute("SELECT key FROM failed_keys WHERE key LIKE ?", (f"{prefix}%",))
    else:
        c.execute("SELECT key FROM failed_keys")
    return [row[0] for row in c.fetchall()]

def clear_failed_keys_db():
    """Clears all failed keys from the database."""
    conn = get_db_conn()
    conn.execute("DELETE FROM failed_keys")
    conn.commit()

# --- Throttling and Rate Limit Handling ---

DYNAMIC_THROTTLE_DELAY = THROTTLE_DELAY
DYNAMIC_THROTTLE_LOCK = threading.Lock()

def dynamic_throttle(api_call):
    """Dynamically adjustable throttling for API calls."""
    global DYNAMIC_THROTTLE_DELAY
    with DYNAMIC_THROTTLE_LOCK:
        delay = DYNAMIC_THROTTLE_DELAY
    time.sleep(delay)
    return api_call()

def handle_rate_limit_error(e):
    """Detects rate-limit errors and increases the delay."""
    global DYNAMIC_THROTTLE_DELAY
    msg = str(e)
    if any(x in msg for x in ["TooManyRequests", "Throttling", "429", "503"]):
        with DYNAMIC_THROTTLE_LOCK:
            DYNAMIC_THROTTLE_DELAY = min(DYNAMIC_THROTTLE_DELAY * 2, 60)  # Max 60s
        logging.warning(f"Rate limit detected, increasing delay to {DYNAMIC_THROTTLE_DELAY:.1f}s")
        return True
    return False

def reset_throttle_delay():
    """Slowly reduces the delay after successful requests."""
    global DYNAMIC_THROTTLE_DELAY
    with DYNAMIC_THROTTLE_LOCK:
        if DYNAMIC_THROTTLE_DELAY > THROTTLE_DELAY:
            DYNAMIC_THROTTLE_DELAY = max(DYNAMIC_THROTTLE_DELAY * 0.9, THROTTLE_DELAY)

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
    """Simple throttling for API calls."""
    time.sleep(delay)
    return api_call()

# --- IBM COS Utility Functions ---

def get_top_level_prefixes(s3, bucket, delimiter="/"):
    """Returns all top-level folders (prefixes) in the bucket."""
    prefixes = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Delimiter=delimiter, PaginationConfig={'PageSize': 1000}):
        for cp in page.get('CommonPrefixes', []):
            prefixes.append(cp['Prefix'])
    return prefixes

# --- Main Batch Processing Functions ---

def process_batch(s3, source_bucket, destination_bucket, batch, batch_number, conn, max_retries=MAX_RETRIES):
    """
    Processes a batch of keys: copies them and updates the database.
    Handles rate limits and retries.
    """
    successful_copies = 0
    keyprotect_crn = os.environ.get("KEY_PROTECT_CRN")
    for key in batch:
        if is_key_copied_db(key):
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
                retry_with_backoff(lambda: dynamic_throttle(copy_object))
                reset_throttle_delay()
                save_copied_key_db(key)
                remove_key_from_failed_keys_db(key)
                successful_copies += 1
                success = True
                break
            except Exception as e:
                if handle_rate_limit_error(e):
                    continue
                error_message = str(e)
                if "InvalidObjectState" in error_message and "Operation is not valid for the source object's storage class" in error_message:
                    save_copied_key_db(key)
                    remove_key_from_failed_keys_db(key)
                    successful_copies += 1
                    success = True
                    break
                else:
                    logging.warning(f"{ERROR_ICON} [{batch_number}] Error moving {key} to archive (attempt {attempt}): {e}")

        if not success:
            save_failed_key_db(key)

    return successful_copies

def copy_objects_in_batches(source_bucket, destination_bucket, prefix, batch_size=BATCH_SIZE):
    """
    Copies only objects directly in the given prefix (not in subfolders).
    """
    s3 = client(
        's3',
        ibm_api_key_id=os.environ['IAM_API_KEY'],
        config=Config(signature_version='oauth'),
        endpoint_url=f"https://s3.{os.environ['REGION']}.cloud-object-storage.appdomain.cloud"
    )
    conn = get_db_conn()
    total_keys = 0
    processed = 0
    failed = 0
    batch = []
    batch_number = 1

    paginate_kwargs = {"Bucket": source_bucket, "Delimiter": "/", "MaxKeys": 1000}
    if prefix:
        paginate_kwargs["Prefix"] = prefix

    for page in s3.get_paginator('list_objects_v2').paginate(**paginate_kwargs):
        # Only files directly in the current prefix (no subfolders)
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Check if the object is directly in the prefix (no further slash after prefix)
            if prefix and "/" in key[len(prefix):]:
                continue  # Skip files in subfolders
            total_keys += 1
            if is_key_copied_db(key):
                continue
            batch.append(key)
            if len(batch) >= batch_size:
                successful = process_batch(s3, source_bucket, destination_bucket, batch, batch_number, conn)
                processed += successful
                failed += (len(batch) - successful)
                batch = []
                batch_number += 1
    # Process the last batch
    if batch:
        successful = process_batch(s3, source_bucket, destination_bucket, batch, batch_number, conn)
        processed += successful
        failed += (len(batch) - successful)

    return processed, total_keys

def retry_failed_keys(source_bucket, destination_bucket, max_retries=MAX_RETRIES):
    """
    Retries copying of failed keys.
    Uses SQLite for status tracking and logging for progress.
    """
    s3 = client(
        's3',
        ibm_api_key_id=os.environ['IAM_API_KEY'],
        config=Config(signature_version='oauth'),
        endpoint_url=f"https://s3.{os.environ['REGION']}.cloud-object-storage.appdomain.cloud"
    )
    conn = get_db_conn()
    keyprotect_crn = os.environ.get("KEY_PROTECT_CRN")
    prefix = os.environ.get("OBJECT_PREFIX", "").strip()
    failed_keys = get_failed_keys_db(prefix=prefix if prefix else None)
    if not failed_keys:
        logging.warning(f"{MAIL_ICON} No failed keys present.")
        return

    total = len(failed_keys)
    logging.warning(f"{RETRY_ICON} Retrying {total} failed objects...")

    for key in failed_keys:
        if is_key_copied_db(key):
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
                save_copied_key_db(key)
                remove_key_from_failed_keys_db(key)
                success = True
                break
            except Exception as e:
                if handle_rate_limit_error(e):
                    continue
                error_message = str(e)
                if "InvalidObjectState" in error_message and "Operation is not valid for the source object's storage class" in error_message:
                    save_copied_key_db(key)
                    remove_key_from_failed_keys_db(key)
                    success = True
                    break
                else:
                    logging.warning(f"{ERROR_ICON} RETRY error moving {key} to archive (attempt {attempt}): {e}")

        if not success:
            save_failed_key_db(key)

    clear_failed_keys_db()
    logging.warning(f"{RETRY_ICON} Retry complete.")

# --- Folder Progress Logging ---

def log_folder_progress(prefix, processed, total):
    """Appends folder processing info to a progress log file."""
    with open(FOLDER_PROGRESS_FILE, "a") as f:
        f.write(f"{prefix}\t{processed}/{total} files processed\n")

# --- Prefix Processing (Iterative) ---

def process_prefix_tree_iterative(s3, source_bucket, destination_bucket, root_prefix="", delimiter="/"):
    """
    Iteratively processes all objects and subfolders under the given prefix (depth-first, no recursion).
    Each prefix is processed as soon as it is discovered.
    Logs progress for each folder after processing.
    """
    stack = [root_prefix]
    while stack:
        current_prefix = stack.pop()
        logging.warning(f"Processing prefix: {current_prefix}")

        # Process files in this prefix and count total files in one go
        before = count_archived_for_prefix_db(current_prefix)
        processed, total_files = copy_objects_in_batches(source_bucket, destination_bucket, current_prefix, batch_size=BATCH_SIZE)
        after = count_archived_for_prefix_db(current_prefix)
        processed = after - before

        # Log folder progress
        log_folder_progress(current_prefix, processed, total_files)
        logging.warning(f"Folder {current_prefix} processed: {processed}/{total_files} files.")

        # Find sub-prefixes (subfolders) and add them to the stack
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=source_bucket, Prefix=current_prefix, Delimiter=delimiter):
            for cp in page.get('CommonPrefixes', []):
                sub_prefix = cp['Prefix']
                stack.append(sub_prefix)

# --- Main Execution ---

if __name__ == '__main__':
    ensure_env()
    init_db()
    try:
        source_bucket = os.environ['SOURCE_BUCKET']
        destination_bucket = os.environ['DESTINATION_BUCKET']
        s3 = client(
            's3',
            ibm_api_key_id=os.environ['IAM_API_KEY'],
            config=Config(signature_version='oauth'),
            endpoint_url=f"https://s3.{os.environ['REGION']}.cloud-object-storage.appdomain.cloud"
        )

        # Get all top-level folders (prefixes)
        top_level_prefixes = get_top_level_prefixes(s3, source_bucket, delimiter="/")

        # Optional: If you want to process files directly in the root, uncomment the next line:
        # process_prefix_tree_iterative(s3, source_bucket, destination_bucket, root_prefix="")

        # Process each top-level folder
        for prefix in top_level_prefixes:
            process_prefix_tree_iterative(s3, source_bucket, destination_bucket, root_prefix=prefix)

        logging.warning("All folders have been processed.")
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM copied_keys")
        total_archived = c.fetchone()[0]
        logging.warning(f"Total archived files: {total_archived}")
        print(f"Total archived files: {total_archived}")
    finally:
        # Am Ende alle fehlgeschlagenen Keys erneut versuchen
        retry_failed_keys(source_bucket, destination_bucket)
        close_db_conn()