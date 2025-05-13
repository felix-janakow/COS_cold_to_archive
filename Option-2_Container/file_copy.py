import time
from ibm_boto3 import client
from ibm_botocore.client import Config
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
import os
from dotenv import load_dotenv
import logging
from tqdm import tqdm

load_dotenv()

COPIED_KEYS_FILE = "copied_keys.txt"
FAILED_KEYS_FILE = "failed_keys.txt"
LOG_FILE = "cos_batch_copy.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Funktion zur Emoji-Unterstützung
def get_icon(emoji, fallback):
    """Gibt ein Emoji oder einen Fallback-Text zurück."""
    use_emojis = os.environ.get("USE_EMOJIS", "true").lower() == "true"
    return emoji if use_emojis else fallback

# Emojis mit Fallback-Texten
CHECK_ICON = get_icon("✅", "[SUCCESS]")
ERROR_ICON = get_icon("❌", "[ERROR]")
RETRY_ICON = get_icon("♻️", "[RETRY]")
INFO_ICON = get_icon("🔄", "[INFO]")
MAIL_ICON = get_icon("📭", "[NO FAILED KEYS]")

def retry_with_backoff(func, max_retries=5, backoff_factor=2):
    """Führt eine Funktion mit exponentiellem Backoff aus."""
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                raise
            time.sleep(backoff_factor ** attempt)

def throttle(api_call, delay=0.1):
    """Drosselt API-Aufrufe."""
    time.sleep(delay)
    return api_call()

def load_copied_keys():
    if not os.path.exists(COPIED_KEYS_FILE):
        return set()
    with open(COPIED_KEYS_FILE, "r") as f:
        return set(line.strip() for line in f.readlines())

def save_copied_key(key):
    with open(COPIED_KEYS_FILE, "a") as f:
        f.write(f"{key}\n")

def save_failed_key(key):
    with open(FAILED_KEYS_FILE, "a") as f:
        f.write(f"{key}\n")

def load_failed_keys():
    if not os.path.exists(FAILED_KEYS_FILE):
        return []
    with open(FAILED_KEYS_FILE, "r") as f:
        return [line.strip() for line in f.readlines()]

def clear_failed_keys():
    open(FAILED_KEYS_FILE, "w").close()

def count_total_keys(s3, bucket):
    total = 0
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        total += len(page.get('Contents', []))
    return total

def process_batch(s3, source_bucket, destination_bucket, batch, batch_number, copied_keys, max_retries=3):
    successful_copies = 0
    for key in batch:
        if key in copied_keys:
            continue

        copy_source = {
            'Bucket': source_bucket,
            'Key': key
        }

        def copy_object():
            s3.copy_object(
                CopySource=copy_source,
                Bucket=destination_bucket,
                Key=key,
                MetadataDirective="REPLACE"
            )

        success = False
        for attempt in range(1, max_retries + 1):
            try:
                retry_with_backoff(lambda: throttle(copy_object))
                tqdm.write(f"{CHECK_ICON} [{batch_number}] {key} erfolgreich kopiert (Versuch {attempt}).")
                logging.info(f"[{batch_number}] {key} erfolgreich kopiert (Versuch {attempt})")
                save_copied_key(key)
                successful_copies += 1
                success = True
                break
            except Exception as e:
                tqdm.write(f"{ERROR_ICON} [{batch_number}] Fehler bei {key} (Versuch {attempt}): {e}")
                logging.warning(f"[{batch_number}] Fehler bei {key} (Versuch {attempt}): {e}")

        if not success:
            save_failed_key(key)

    return successful_copies

def copy_objects_in_batches(source_bucket, destination_bucket, batch_size=1000):
    s3 = client(
        's3',
        ibm_api_key_id=os.environ['IAM_API_KEY'],
        config=Config(signature_version='oauth'),
        endpoint_url=f"https://s3.{os.environ['REGION']}.cloud-object-storage.appdomain.cloud"
    )

    copied_keys = load_copied_keys()
    total_keys = count_total_keys(s3, source_bucket)
    total_to_process = total_keys - len(copied_keys)

    if total_to_process <= 0:
        tqdm.write(f"{CHECK_ICON} Alle Dateien wurden bereits verarbeitet.")
        return

    tqdm.write(f"{INFO_ICON} Gesamt zu verarbeitende Dateien: {total_to_process}")

    paginator = s3.get_paginator('list_objects_v2')
    batch = []
    batch_number = 1
    processed = 0
    failed = 0

    with tqdm(total=total_to_process, desc=f"{INFO_ICON} Verarbeitung", unit="obj") as pbar:
        for page in paginator.paginate(Bucket=source_bucket):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key in copied_keys:
                    continue

                batch.append(key)

                if len(batch) >= batch_size:
                    tqdm.write(f"{INFO_ICON} Verarbeite Batch {batch_number} mit {len(batch)} Objekten...")
                    successful = process_batch(s3, source_bucket, destination_bucket, batch, batch_number, copied_keys)
                    processed += successful
                    failed += (batch_size - successful)
                    batch = []
                    batch_number += 1
                    pbar.update(successful)

        # Letzten Rest verarbeiten
        if batch:
            tqdm.write(f"{INFO_ICON} Verarbeite letzten Batch {batch_number} mit {len(batch)} Objekten...")
            successful = process_batch(s3, source_bucket, destination_bucket, batch, batch_number, copied_keys)
            processed += successful
            failed += (len(batch) - successful)
            pbar.update(successful)

    tqdm.write(f"{CHECK_ICON} Verarbeitung abgeschlossen. Erfolgreich verarbeitet: {processed} von {total_to_process}")
    tqdm.write(f"{ERROR_ICON} {failed} von {total_to_process} Objekten fehlgeschlagen")
    logging.info(f"Verarbeitung abgeschlossen. Erfolgreich verarbeitet: {processed} von {total_to_process}")
    logging.info(f"Fehlgeschlagene Objekte: {failed} von {total_to_process}")

def retry_failed_keys(source_bucket, destination_bucket, max_retries=3):
    s3 = client(
        's3',
        ibm_api_key_id=os.environ['IAM_API_KEY'],
        config=Config(signature_version='oauth'),
        endpoint_url=f"https://s3.{os.environ['REGION']}.cloud-object-storage.appdomain.cloud"
    )

    failed_keys = load_failed_keys()
    if not failed_keys:
        tqdm.write(f"{MAIL_ICON} Keine fehlgeschlagenen Keys vorhanden.")
        return

    copied_keys = load_copied_keys()
    remaining_keys = []
    total = len(failed_keys)

    tqdm.write(f"{RETRY_ICON} Wiederhole {total} fehlgeschlagene Objekte...")

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
                    s3.copy_object(
                        CopySource=copy_source,
                        Bucket=destination_bucket,
                        Key=key,
                        MetadataDirective="REPLACE"
                    )
                    tqdm.write(f"{CHECK_ICON} RETRY: {key} erfolgreich kopiert (Versuch {attempt})")
                    logging.info(f"RETRY: {key} erfolgreich kopiert (Versuch {attempt})")
                    save_copied_key(key)
                    success = True
                    break
                except Exception as e:
                    tqdm.write(f"{ERROR_ICON} RETRY-Fehler bei {key} (Versuch {attempt}): {e}")
                    logging.warning(f"RETRY: Fehler bei {key} (Versuch {attempt}): {e}")

            if not success:
                remaining_keys.append(key)

            pbar.update(1)

    with open(FAILED_KEYS_FILE, "w") as f:
        for key in remaining_keys:
            f.write(f"{key}\n")

    tqdm.write(f"{RETRY_ICON} Retry abgeschlossen. Noch übrig: {len(remaining_keys)}")
    logging.info(f"Retry abgeschlossen. Übrig gebliebene Fehler: {len(remaining_keys)}")

if __name__ == '__main__':
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    mode = os.environ.get("COPY_MODE", "normal")

    if mode == "retry":
        retry_failed_keys(source_bucket, destination_bucket)
    else:
        copy_objects_in_batches(source_bucket, destination_bucket, batch_size=10)
