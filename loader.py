import os
import sys
import random
import logging
from dotenv import load_dotenv
from ibm_boto3 import client
from ibm_botocore.client import Config
import sqlite3
import json
import time
from datetime import datetime, timedelta
import concurrent.futures
import queue
import psutil

################### EXCEPTION HANDLING SETUP ###################
# Setup exception handling for different client libraries
try:
    from ibm_botocore.exceptions import ClientError
except ImportError:
    try:
        from botocore.exceptions import ClientError
    except ImportError:
        # Generic fallback if neither library is available
        class ClientError(Exception):
            def __init__(self, error, operation_name):
                self.response = {'Error': {'Code': error}}
                self.operation_name = operation_name

################### LOGGING CONFIGURATION ###################
# Configure logging with appropriate handlers based on execution mode
if len(sys.argv) > 1 and sys.argv[1] == "stats":
    log_file = "archive_stats_latest.log"  # Fixed name for stats command
    if os.path.exists(log_file):
        os.remove(log_file)
else:
    # Use timestamped log file for other operations
    log_file = f"archive_process_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

################### DATABASE OPTIMIZATION ###################
# Optimize SQLite database performance
def optimize_db_connection(conn):
    """Apply performance optimizations to database connection."""
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -20000")  # 20MB cache
    conn.execute("PRAGMA mmap_size = 30000000000")  # 30GB memory map if available

def create_indexes(conn):
    """Create indexes on main tables to improve query performance."""
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cos_objects_key ON cos_objects(key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_copied_keys_key ON copied_keys(key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_failed_keys_key ON failed_keys(key)")

################### RETRY MECHANISM ###################
# Implement retry logic with exponential backoff
def retry_with_backoff(func, *args, **kwargs):
    """Execute a function with exponential backoff retry logic."""
    max_retries = 10
    base_delay = 1
    max_delay = 300  # 5 minutes max delay
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            # Rate limit errors
            if error_code in ['TooManyRequests', 'SlowDown', 'RequestLimitExceeded', '429']:
                delay = min(max_delay, (base_delay * 2 ** attempt) + random.uniform(0, 1))
                logger.warning(f"Rate limited, attempt {attempt+1}/{max_retries}, waiting {delay:.2f} seconds...")
                time.sleep(delay)
            # Handle network/timeout errors
            elif error_code in ['RequestTimeout', 'InternalError', 'ServiceUnavailable']:
                delay = min(max_delay, (base_delay * 2 ** attempt) + random.uniform(0, 1))
                logger.warning(f"Service unavailable, attempt {attempt+1}/{max_retries}, waiting {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                # Other errors should be raised immediately
                logger.error(f"Unhandled error: {str(e)}")
                raise
        except Exception as e:
            # Non-ClientError exceptions
            logger.error(f"Unexpected error: {str(e)}")
            raise
            
    # If we get here, we've exhausted our retries
    raise Exception(f"Maximum retry attempts ({max_retries}) reached")

################### OBJECT LISTING ###################
# List all objects from COS bucket to SQLite database
def list_cos_objects_to_sqlite(bucket_name, db_path, continuation_token=None):
    """List objects in COS bucket and store keys in SQLite database."""
    start_time = time.time()
    total_objects = 0
    
    logger.info(f"Starting listing objects from bucket: {bucket_name}")
    
    # Build up the S3 Client using environment variables
    cos = client(
        's3',
        ibm_api_key_id=os.environ.get('IAM_API_KEY'),
        config=Config(
            signature_version='oauth',
            max_pool_connections=100,  # Increase connection pool
            retries={'max_attempts': 3},  # Let boto3 handle basic retries
            connect_timeout=60,  # Increase timeouts for large operations
            read_timeout=60
        ),
        endpoint_url=f"https://s3.{os.environ.get('REGION')}.cloud-object-storage.appdomain.cloud"
    )

    # SQLite DB connection
    with sqlite3.connect(db_path, timeout=60) as conn:
        optimize_db_connection(conn)
        c = conn.cursor()
        
        # Create needed tables
        c.execute("""
            CREATE TABLE IF NOT EXISTS cos_objects (
                key TEXT PRIMARY KEY
            )
        """)
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS continuation_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                continuation_token TEXT,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create stats table for monitoring progress
        c.execute("""
            CREATE TABLE IF NOT EXISTS listing_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_objects INTEGER DEFAULT 0,
                start_time TEXT,
                last_update_time TEXT,
                finished_time TEXT
            )
        """)
        
        # Update or insert stats
        c.execute("""
            INSERT OR REPLACE INTO listing_stats 
            (id, start_time, last_update_time) 
            VALUES (1, datetime('now'), datetime('now'))
        """)
        conn.commit()

        # Get the saved continuation token if not provided
        if not continuation_token:
            c.execute("SELECT continuation_token FROM continuation_state WHERE id = 1")
            result = c.fetchone()
            if result and result[0]:
                continuation_token = result[0]
                logger.info(f"Resuming listing with saved continuation token")
                
                # Get already processed count
                c.execute("SELECT total_objects FROM listing_stats WHERE id = 1")
                result = c.fetchone()
                if result:
                    total_objects = result[0]
                    logger.info(f"Already processed {total_objects} objects in previous runs")

        # Create transaction batches to avoid enormous transactions
        tx_batch_size = 10000
        objects_in_current_tx = 0
        objects_processed_since_cursor_reset = 0
        
        try:
            # Initialize response outside the loop
            more_objects = True
            
            while more_objects:
                # Prepare parameters for listing objects
                list_objects_params = {
                    "Bucket": bucket_name,
                    "MaxKeys": 1000
                }
                # Add continuation token if we have one
                if continuation_token:
                    list_objects_params["ContinuationToken"] = continuation_token
                
                # Get the objects with retry logic
                def list_objects():
                    return cos.list_objects_v2(**list_objects_params)
                    
                try:
                    response = retry_with_backoff(list_objects)
                    
                    # Process contents
                    batch_count = 0
                    for obj in response.get("Contents", []):
                        c.execute(
                            "INSERT OR REPLACE INTO cos_objects (key) VALUES (?)",
                            (obj["Key"],)
                        )
                        batch_count += 1
                        total_objects += 1
                        objects_in_current_tx += 1
                        objects_processed_since_cursor_reset += 1
                        
                        # Commit in smaller batches to avoid huge transactions
                        if objects_in_current_tx >= tx_batch_size:
                            conn.commit()
                            # Update stats
                            c.execute("""
                                UPDATE listing_stats 
                                SET total_objects = ?, last_update_time = datetime('now')
                                WHERE id = 1
                            """, (total_objects,))
                            conn.commit()
                            objects_in_current_tx = 0
                    
                    logger.info(f"Processed batch with {batch_count} objects. Total processed: {total_objects}")
                    
                    # Save progress after processing each batch
                    if response.get("NextContinuationToken"):
                        continuation_token = response["NextContinuationToken"]
                        c.execute("""
                            INSERT OR REPLACE INTO continuation_state (id, continuation_token, last_updated) 
                            VALUES (1, ?, CURRENT_TIMESTAMP)
                        """, (continuation_token,))
                        conn.commit()
                        logger.info(f"Continuation token saved.")
                    else:
                        more_objects = False
                        logger.info("Reached end of bucket listing.")
                
                except Exception as e:
                    # If we fail during a batch, save what we have and re-raise
                    logger.error(f"Error during object listing: {str(e)}")
                    if objects_in_current_tx > 0:
                        conn.commit()
                        logger.info(f"Saved progress before error, total processed: {total_objects}")
                    raise
                    
                # Reset cursor periodically
                if objects_processed_since_cursor_reset >= 100000:
                    # Commit any pending changes first
                    if objects_in_current_tx > 0:
                        conn.commit()
                        objects_in_current_tx = 0
                    
                    # Close and recreate cursor
                    logger.debug("Recycling database cursor after 100,000 objects")
                    c.close()
                    c = conn.cursor()
                    objects_processed_since_cursor_reset = 0
                    
        except KeyboardInterrupt:
            logger.warning("Process interrupted. Progress has been saved.")
            conn.commit()
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            conn.commit()
            raise
        finally:
            # Make sure changes are committed
            if objects_in_current_tx > 0:
                conn.commit()
                
            # Update final stats
            c.execute("""
                UPDATE listing_stats 
                SET total_objects = ?, last_update_time = datetime('now')
                WHERE id = 1
            """, (total_objects,))
            conn.commit()
    
    # Clear the continuation token when complete if we finished
    if not more_objects:
        end_time = time.time()
        duration = end_time - start_time
        
        with sqlite3.connect(db_path) as conn:
            optimize_db_connection(conn)
            c = conn.cursor()
            c.execute("DELETE FROM continuation_state WHERE id = 1")
            
            # Update stats with finished time
            c.execute("""
                UPDATE listing_stats 
                SET finished_time = datetime('now')
                WHERE id = 1
            """)
            conn.commit()
            
        logger.info(f"Listing completed successfully. Processed {total_objects} objects in {duration:.2f} seconds.")
        
        # Create tables for tracking and clean up cos_objects
        cleanup_after_listing(db_path)

################### POST-LISTING CLEANUP ###################
# Prepare database for archiving operations
def cleanup_after_listing(db_path):
    """
    Create tracking tables and remove already processed objects from cos_objects.
    """
    logger.info("Starting post-listing cleanup...")
    
    with sqlite3.connect(db_path) as conn:
        optimize_db_connection(conn)
        c = conn.cursor()
        
        # Create tables to track successful and failed objects
        c.execute("""
            CREATE TABLE IF NOT EXISTS copied_keys (
                key TEXT PRIMARY KEY,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS failed_keys (
                key TEXT PRIMARY KEY,
                error TEXT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                attempts INTEGER DEFAULT 1
            )
        """)
        
        # Create indexes for performance
        create_indexes(conn)
    
    # Update schema to ensure all required columns exist
    update_database_schema(db_path)
    
    # Reopen connection after schema update
    with sqlite3.connect(db_path) as conn:
        optimize_db_connection(conn)
        c = conn.cursor()
        
        # Get counts before cleanup
        c.execute("SELECT COUNT(*) FROM cos_objects")
        before_count = c.fetchone()[0]
        
        logger.info(f"Starting cleanup with {before_count} objects in cos_objects table")
        
        # Remove entries from cos_objects that are already in copied_keys (in batches to avoid locking)
        c.execute("SELECT COUNT(*) FROM copied_keys")
        copied_total = c.fetchone()[0]
        
        if copied_total > 0:
            logger.info(f"Removing {copied_total} already processed objects from cos_objects")
            
            # For very large tables, use keyset pagination
            last_key = ""
            batch_size = 100000
            deleted = 0
            
            while True:
                # Use keyset pagination (WHERE key > last_processed_key ORDER BY key LIMIT batch_size)
                c.execute("""
                    DELETE FROM cos_objects 
                    WHERE key IN (
                        SELECT o.key FROM cos_objects o
                        JOIN copied_keys c ON c.key = o.key
                        WHERE o.key > ?
                        ORDER BY o.key
                        LIMIT ?
                    )
                """, (last_key, batch_size))
                
                deleted_batch = c.rowcount
                deleted += deleted_batch
                
                if deleted_batch > 0:
                    # Get the last processed key for the next batch
                    c.execute("""
                        SELECT MAX(key) FROM (
                            SELECT o.key FROM cos_objects o
                            JOIN copied_keys c ON c.key = o.key
                            WHERE o.key > ?
                            ORDER BY o.key
                            LIMIT ?
                        )
                    """, (last_key, batch_size))
                    
                    result = c.fetchone()
                    if result[0]:
                        last_key = result[0]
                
                conn.commit()
                
                logger.info(f"Removed batch of {deleted_batch} objects, total removed: {deleted}")
                
                if deleted_batch < batch_size:
                    break
        
        # Get counts for reporting
        c.execute("SELECT COUNT(*) FROM cos_objects")
        remaining_count = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM copied_keys")
        copied_count = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM failed_keys")
        failed_count = c.fetchone()[0]
    
    logger.info(f"Database prepared for archiving:")
    logger.info(f"  - {remaining_count} objects remaining to be archived")
    logger.info(f"  - {copied_count} objects already archived")
    logger.info(f"  - {failed_count} objects previously failed")

################### MAIN ARCHIVING PROCESS ###################
# Archive objects using multi-threaded processing
def archive_objects(bucket_name, db_path, batch_size=100, max_workers=5):
    """
    Performs in-place copy operations on objects using multiple threads.
    
    Args:
        bucket_name (str): Name of the bucket containing objects to archive
        db_path (str): Path to SQLite database containing object keys
        batch_size (int): Number of objects to fetch in each batch
        max_workers (int): Maximum number of worker threads (capped at 5)
    """
    overall_start_time = time.time()
    overall_success = 0
    overall_failures = 0
    total_batches = 0
    
    # Initial delay between operations for throttling
    current_delay = 0.1  # Start with a small delay
    
    # Cap batch size and worker count to reasonable values
    batch_size = min(batch_size, 1000)
    max_workers = min(max_workers, 5)  # Prevent system overload
    
    logger.info(f"Starting threaded archiving process for bucket: {bucket_name} with {max_workers} workers")
    logger.info(f"Initial throttling delay: {current_delay}s")
    
    # Build up the S3 Client using environment variables
    cos = client(
        's3',
        ibm_api_key_id=os.environ.get('IAM_API_KEY'),
        config=Config(
            signature_version='oauth',
            max_pool_connections=max(50, max_workers * 5),  # Increase connection pool based on worker count
            retries={'max_attempts': 0},  # We'll handle retries ourselves
            connect_timeout=60,
            read_timeout=60
        ),
        endpoint_url=f"https://s3.{os.environ.get('REGION')}.cloud-object-storage.appdomain.cloud"
    )
    
    # Create a thread-safe queue for results
    result_queue = queue.Queue()
    
    def archive_object(obj_key, throttle_delay):
        """Worker function to archive a single object and report result"""
        try:
            # Ensure that obj_key is a string
            obj_key = str(obj_key)
            
            # Apply throttling delay before operation
            if throttle_delay > 0:
                time.sleep(throttle_delay)
            
            # Perform in-place copy to trigger archiving with retry logic
            copy_source = {'Bucket': bucket_name, 'Key': obj_key}
            
            def do_copy():
                return cos.copy_object(
                    Bucket=bucket_name,
                    Key=obj_key,
                    CopySource=copy_source,
                    MetadataDirective='REPLACE'  # Keep the original metadata
                )
            
            # Execute with retry logic
            retry_with_backoff(do_copy)
            
            # Report success
            result_queue.put(('success', obj_key, None))
            return True
            
        except Exception as e:
            error_message = str(e)
            # Report failure
            result_queue.put(('failure', obj_key, error_message))
            return False
    
    # Continue processing until we have no more objects or manual interruption
    try:
        while True:
            # Connect to the SQLite database (new connection for each batch to avoid issues)
            with sqlite3.connect(db_path, timeout=60) as conn:
                optimize_db_connection(conn)
                c = conn.cursor()
                
                # Ensure required tables and stats tracking
                c.execute("""
                    CREATE TABLE IF NOT EXISTS cos_objects (
                        key TEXT PRIMARY KEY
                    )
                """)
                
                c.execute("""
                    CREATE TABLE IF NOT EXISTS copied_keys (
                        key TEXT PRIMARY KEY,
                        timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                c.execute("""
                    CREATE TABLE IF NOT EXISTS failed_keys (
                        key TEXT PRIMARY KEY,
                        error TEXT,
                        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                        attempts INTEGER DEFAULT 1
                    )
                """)
                
                # Create indexes if they don't exist
                create_indexes(conn)
                
                # Create archive stats table
                c.execute("""
                    CREATE TABLE IF NOT EXISTS archive_stats (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        total_success INTEGER DEFAULT 0,
                        total_failures INTEGER DEFAULT 0,
                        start_time TEXT,
                        last_update_time TEXT,
                        finished_time TEXT
                    )
                """)
                
                # Initialize or update stats
                c.execute("""
                    INSERT OR REPLACE INTO archive_stats 
                    (id, total_success, total_failures, start_time, last_update_time) 
                    VALUES (
                        1, 
                        COALESCE((SELECT total_success FROM archive_stats WHERE id=1), 0),
                        COALESCE((SELECT total_failures FROM archive_stats WHERE id=1), 0),
                        COALESCE((SELECT start_time FROM archive_stats WHERE id=1), datetime('now')),
                        datetime('now')
                    )
                """)
                conn.commit()
                
                # Get current counts from stats
                c.execute("SELECT total_success, total_failures FROM archive_stats WHERE id=1")
                result = c.fetchone()
                total_success, total_failures = result if result else (0, 0)
                
                # Get objects directly from cos_objects table
                c.execute("""
                    SELECT key FROM cos_objects
                    LIMIT ?
                """, (batch_size,))
                
                # Extract the strings from the tuples
                objects_to_archive = [row[0] for row in c.fetchall()]
                
                if not objects_to_archive:
                    logger.info("No more objects to archive. Process completed.")
                    
                    # Record completion
                    c.execute("""
                        UPDATE archive_stats 
                        SET finished_time = datetime('now')
                        WHERE id = 1
                    """)
                    conn.commit()
                    break  # Exit the continuous loop
                
                logger.info(f"Batch {total_batches + 1}: Processing {len(objects_to_archive)} objects with {max_workers} threads (throttle: {current_delay:.3f}s)")
                
                batch_start_time = time.time()
                success_count = 0
                failure_count = 0
                
                ############## PARALLEL PROCESSING WITH THREAD POOL ##############
                # Process objects in parallel using a thread pool
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Start all the archive tasks - pass strings and current throttle delay
                    future_to_key = {executor.submit(archive_object, key, current_delay): key for key in objects_to_archive}
                    
                    # Process results from the queue while waiting for tasks to complete
                    remaining_tasks = len(future_to_key)
                    success_batch_count = 0
                    failure_batch_count = 0

                    while remaining_tasks > 0:
                        # Process up to 10 results at a time without logging each one
                        for _ in range(min(10, remaining_tasks)):
                            try:
                                # Non-blocking queue check with timeout
                                result_type, obj_key, error = result_queue.get(timeout=0.1)
                                
                                # Update database with result
                                if result_type == 'success':
                                    c.execute(
                                        "INSERT OR REPLACE INTO copied_keys (key, timestamp) VALUES (?, datetime('now'))",
                                        (obj_key,)
                                    )
                                    c.execute("DELETE FROM cos_objects WHERE key = ?", (obj_key,))
                                    success_count += 1
                                    success_batch_count += 1
                                    
                                else:  # failure
                                    c.execute(
                                        "INSERT OR REPLACE INTO failed_keys (key, error, timestamp, attempts) VALUES (?, ?, datetime('now'), COALESCE((SELECT attempts FROM failed_keys WHERE key = ?) + 1, 1))",
                                        (obj_key, error, obj_key)
                                    )
                                    c.execute("DELETE FROM cos_objects WHERE key = ?", (obj_key,))
                                    failure_count += 1
                                    failure_batch_count += 1
                                    # Only log individual failures at warning level
                                    if len(error) > 100:
                                        error = error[:100] + "..."
                                    logger.warning(f"Failed to archive: {obj_key} - {error}")
                                    
                                # Mark task as processed
                                result_queue.task_done()
                                remaining_tasks -= 1
                                
                                # Commit periodically
                                if (success_count + failure_count) % 20 == 0:
                                    conn.commit()
                                    
                                    # Log progress periodically instead of per object
                                    if (success_count + failure_count) % 100 == 0:
                                        logger.info(f"Progress: {success_count + failure_count}/{len(objects_to_archive)} objects processed in this batch")
                            
                            except queue.Empty:
                                # No results ready yet
                                break
                        
                        # Check if we're done with all tasks
                        if remaining_tasks == 0:
                            break
                            
                        # Small sleep to prevent CPU spinning while waiting for results
                        time.sleep(0.1)
                        
                        # Check for completed futures without forcing a timeout
                        completed_futures = [future for future in future_to_key.keys() 
                                            if future.done()]
                        
                        # Process all completed futures
                        for future in completed_futures:
                            del future_to_key[future]
                
                ############## BATCH COMPLETION AND STATISTICS ##############
                # Calculate success rate for this batch for adaptive throttling
                batch_total = success_count + failure_count
                success_rate = success_count / batch_total if batch_total > 0 else 0
                
                # Update throttling based on success rate
                previous_delay = current_delay
                current_delay = adaptive_throttle(success_rate, current_delay)
                
                if previous_delay != current_delay:
                    logger.info(f"Adaptive throttling: Success rate {success_rate:.2f}, adjusting delay from {previous_delay:.3f}s to {current_delay:.3f}s")
                
                # Final update of stats for this batch
                overall_success += success_count
                overall_failures += failure_count
                total_batches += 1
                
                # Update stats
                c.execute("""
                    UPDATE archive_stats 
                    SET total_success = total_success + ?, 
                        total_failures = total_failures + ?, 
                        last_update_time = datetime('now')
                    WHERE id = 1
                """, (success_count, failure_count))
                
                conn.commit()
                
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                rate = len(objects_to_archive) / batch_duration if batch_duration > 0 else 0
                
                # Log batch stats
                logger.info(f"Batch {total_batches} completed in {batch_duration:.2f} seconds.")
                logger.info(f"Batch processing rate: {rate:.2f} objects per second.")
                logger.info(f"Batch results - Success: {success_count}, Failed: {failure_count}")
                logger.info(f"Overall totals - Success: {overall_success}, Failed: {overall_failures}")
                
                ############## PROGRESS ESTIMATION ##############
                # Calculate estimated time remaining
                elapsed_time = time.time() - overall_start_time
                objects_processed = overall_success + overall_failures
                
                # Get total objects remaining - use the same connection that's still open
                c.execute("SELECT COUNT(*) FROM cos_objects")
                objects_remaining = c.fetchone()[0]
                
                if objects_processed > 0 and elapsed_time > 0:
                    rate_overall = objects_processed / elapsed_time
                    if rate_overall > 0:
                        est_seconds_remaining = objects_remaining / rate_overall
                        est_hours_remaining = est_seconds_remaining / 3600
                        est_days_remaining = est_hours_remaining / 24
                        
                        logger.info(f"Progress: {objects_processed} processed, {objects_remaining} remaining")
                        logger.info(f"Estimated time remaining: {est_days_remaining:.2f} days ({est_hours_remaining:.2f} hours)")
                
                # Optional: Add a small delay between batches
                time.sleep(1)
    
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user.")
        # Drain the queue to log any unprocessed results
        drain_result_queue(result_queue)  # Pass the queue explicitly
        elapsed_time = time.time() - overall_start_time
        logger.info(f"Process ran for {elapsed_time/3600:.2f} hours.")
        logger.info(f"Processed {overall_success + overall_failures} objects (Success: {overall_success}, Failed: {overall_failures})")
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        # Drain the queue to log any unprocessed results
        drain_result_queue(result_queue)  # Pass the queue explicitly
        elapsed_time = time.time() - overall_start_time
        logger.info(f"Process ran for {elapsed_time/3600:.2f} hours before error.")
        logger.info(f"Processed {overall_success + overall_failures} objects (Success: {overall_success}, Failed: {overall_failures})")
        raise
    
    # Final summary
    elapsed_time = time.time() - overall_start_time
    logger.info("=" * 60)
    logger.info(f"Archiving process completed.")
    logger.info(f"Total runtime: {elapsed_time/3600:.2f} hours")
    logger.info(f"Total objects processed: {overall_success + overall_failures}")
    logger.info(f"Total successful: {overall_success}")
    logger.info(f"Total failed: {overall_failures}")
    logger.info("=" * 60)

################### DATABASE SCHEMA MAINTENANCE ###################
# Update database schema to ensure all required columns exist
def update_database_schema(db_path):
    """Update existing database schema to match current requirements."""
    logger.info("Checking database schema and updating if needed...")
    
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        
        # Check copied_keys table
        c.execute("PRAGMA table_info(copied_keys)")
        columns = {row[1] for row in c.fetchall()}
        if "timestamp" not in columns:
            logger.info("Adding timestamp column to copied_keys table")
            # Add column without default value first
            c.execute("ALTER TABLE copied_keys ADD COLUMN timestamp TEXT")
            # Then update all existing rows to set current timestamp
            c.execute("UPDATE copied_keys SET timestamp = datetime('now') WHERE timestamp IS NULL")
        
        # Check failed_keys table
        c.execute("PRAGMA table_info(failed_keys)")
        columns = {row[1] for row in c.fetchall()}
        if "error" not in columns:
            logger.info("Adding error column to failed_keys table")
            c.execute("ALTER TABLE failed_keys ADD COLUMN error TEXT")
        
        if "timestamp" not in columns:
            logger.info("Adding timestamp column to failed_keys table")
            # Add column without default value first
            c.execute("ALTER TABLE failed_keys ADD COLUMN timestamp TEXT")
            # Then update all existing rows to set current timestamp
            c.execute("UPDATE failed_keys SET timestamp = datetime('now') WHERE timestamp IS NULL")
        
        if "attempts" not in columns:
            logger.info("Adding attempts column to failed_keys table")
            # Add column with simple constant default
            c.execute("ALTER TABLE failed_keys ADD COLUMN attempts INTEGER DEFAULT 1")
        
        conn.commit()
        
    logger.info("Database schema update completed")

################### ADAPTIVE THROTTLING ###################
# Adjust throttling based on success rate
def adaptive_throttle(success_rate, current_delay):
    """
    Adjust throttling based on success rate.
    Returns a new delay value in seconds.
    """
    if success_rate > 0.95:  # Very successful
        return max(0.05, current_delay * 0.9)  # Reduce delay
    elif success_rate < 0.8:  # Lots of errors
        return min(1.0, current_delay * 1.5)   # Increase delay
    else:
        return current_delay  # Keep current delay

################### CHECKPOINTING FUNCTIONS ###################
# Create checkpoint files for recovery
def write_checkpoint(stats_dict, prefix="archive_checkpoint"):
    """Write a checkpoint file with current statistics."""
    filename = f"{prefix}_{int(time.time())}.json"
    with open(filename, "w") as f:
        json.dump(stats_dict, f)
    
    # Keep only the 5 most recent checkpoints
    import glob
    checkpoints = sorted(glob.glob(f"{prefix}_*.json"))
    for old_checkpoint in checkpoints[:-5]:
        os.remove(old_checkpoint)

################### RESOURCE OPTIMIZATION ###################
# Functions to optimize resource usage
def adjust_thread_count(current_workers, success_rate, queue_length):
    """Dynamically adjust worker count based on system performance"""
    if success_rate < 0.7:  # Too many errors
        return max(2, int(current_workers * 0.8))  # Reduce workers
    elif success_rate > 0.95 and queue_length > current_workers * 2:
        return min(5, current_workers + 1)  # Increase workers, now limited to 5
    return current_workers

def get_optimal_thread_count():
    """Calculate optimal thread count based on system resources"""
    cpu_count = psutil.cpu_count(logical=True)
    memory_gb = psutil.virtual_memory().total / (1024**3)
    # Balance between CPU and memory consideration
    return min(max(2, cpu_count - 1), int(memory_gb / 2), 20)

################### ERROR HANDLING ###################
# Process any remaining results in the queue before exiting
def drain_result_queue(result_queue_param=None):
    """Process any remaining results in the queue before exiting"""
    try:
        # Use the parameter if provided, otherwise try to use the global
        queue_to_drain = result_queue_param
        
        if queue_to_drain is None:
            # Only try to access the global if no parameter was provided
            try:
                global result_queue
                queue_to_drain = result_queue
            except NameError:
                logger.error("Result queue not available")
                return
        
        while not queue_to_drain.empty():
            result_type, obj_key, error = queue_to_drain.get_nowait()
            # Log results that couldn't be processed
            logger.warning(f"Unprocessed result: {result_type} for {obj_key}")
            queue_to_drain.task_done()
    except Exception as e:
        logger.error(f"Error draining result queue: {str(e)}")

################### OBJECT LISTING WITH DATE FILTER ###################
# List objects from COS bucket to SQLite database, filtering by creation date
def list_cos_objects_to_sqlite_with_date_filter(bucket_name, db_path, cutoff_date="2025-07-13", continuation_token=None):
    """
    List objects in COS bucket created before a specific date and store keys in SQLite database.
    """
    start_time = time.time()
    total_objects = 0
    filtered_objects = 0
    
    # Import specific timezone module
    from datetime import timezone
    
    # Convert cutoff_date string to datetime object and make it timezone-aware
    if isinstance(cutoff_date, str):
        # First, create a datetime at midnight on the cutoff date
        year, month, day = map(int, cutoff_date.split('-'))
        cutoff_date = datetime(year, month, day, 23, 59, 59)
        # Make it timezone-aware (UTC)
        cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
        logger.info(f"Using cutoff date: {cutoff_date.isoformat()}")
    
    logger.info(f"Starting listing objects from bucket: {bucket_name} (created before {cutoff_date.strftime('%Y-%m-%d')})")
    
    # Build up the S3 Client using environment variables
    cos = client(
        's3',
        ibm_api_key_id=os.environ.get('IAM_API_KEY'),
        config=Config(
            signature_version='oauth',
            max_pool_connections=100,
            retries={'max_attempts': 3},
            connect_timeout=60,
            read_timeout=60
        ),
        endpoint_url=f"https://s3.{os.environ.get('REGION')}.cloud-object-storage.appdomain.cloud"
    )

    # SQLite DB connection
    with sqlite3.connect(db_path, timeout=60) as conn:
        optimize_db_connection(conn)
        c = conn.cursor()
        
        # Create needed tables
        c.execute("""
            CREATE TABLE IF NOT EXISTS cos_objects (
                key TEXT PRIMARY KEY,
                last_modified TEXT
            )
        """)
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS continuation_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                continuation_token TEXT,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create stats table for monitoring progress
        c.execute("""
            CREATE TABLE IF NOT EXISTS listing_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_objects INTEGER DEFAULT 0,
                filtered_objects INTEGER DEFAULT 0,
                start_time TEXT,
                last_update_time TEXT,
                finished_time TEXT,
                cutoff_date TEXT
            )
        """)
        
        # Update or insert stats
        c.execute("""
            INSERT OR REPLACE INTO listing_stats 
            (id, total_objects, filtered_objects, start_time, last_update_time, cutoff_date) 
            VALUES (1, 0, 0, datetime('now'), datetime('now'), ?)
        """, (cutoff_date.isoformat(),))
        conn.commit()

        # Get the saved continuation token if not provided
        if not continuation_token:
            c.execute("SELECT continuation_token FROM continuation_state WHERE id = 1")
            result = c.fetchone()
            if result and result[0]:
                continuation_token = result[0]
                logger.info(f"Resuming listing with saved continuation token")
                
                # Get already processed counts
                c.execute("SELECT total_objects, filtered_objects FROM listing_stats WHERE id = 1")
                result = c.fetchone()
                if result:
                    total_objects = result[0]
                    filtered_objects = result[1]
                    logger.info(f"Already processed {total_objects} objects in previous runs (filtered: {filtered_objects})")

        # Create transaction batches to avoid enormous transactions
        tx_batch_size = 10000
        objects_in_current_tx = 0
        objects_processed_since_cursor_reset = 0
        
        try:
            # Initialize response outside the loop
            more_objects = True
            
            while more_objects:
                # Prepare parameters for listing objects
                list_objects_params = {
                    "Bucket": bucket_name,
                    "MaxKeys": 1000
                }
                # Add continuation token if we have one
                if continuation_token:
                    list_objects_params["ContinuationToken"] = continuation_token
                
                # Get the objects with retry logic
                def list_objects():
                    return cos.list_objects_v2(**list_objects_params)
                    
                try:
                    response = retry_with_backoff(list_objects)
                    
                    # Process contents
                    batch_count = 0
                    filtered_batch_count = 0
                    
                    for obj in response.get("Contents", []):
                        # Get the object's last modified date
                        obj_last_modified = obj.get("LastModified")
                        
                        # Convert to datetime if it's not already
                        if isinstance(obj_last_modified, str):
                            obj_last_modified = datetime.fromisoformat(obj_last_modified.replace('Z', '+00:00'))
                            
                        # Add all objects to total count for reporting
                        total_objects += 1
                        
                        # Only insert objects created before the cutoff date
                        if obj_last_modified < cutoff_date:
                            c.execute(
                                "INSERT OR REPLACE INTO cos_objects (key, last_modified) VALUES (?, ?)",
                                (obj["Key"], obj_last_modified.isoformat())
                            )
                            filtered_objects += 1
                            filtered_batch_count += 1
                            objects_in_current_tx += 1
                            
                        batch_count += 1
                        objects_processed_since_cursor_reset += 1
                        
                        # Commit in smaller batches to avoid huge transactions
                        if objects_in_current_tx >= tx_batch_size:
                            conn.commit()
                            # Update stats
                            c.execute("""
                                UPDATE listing_stats 
                                SET total_objects = ?, filtered_objects = ?, last_update_time = datetime('now')
                                WHERE id = 1
                            """, (total_objects, filtered_objects))
                            conn.commit()
                            objects_in_current_tx = 0
                    
                    logger.info(f"Processed batch with {batch_count} objects, kept {filtered_batch_count} within date range.")
                    logger.info(f"Total processed: {total_objects}, filtered: {filtered_objects}")
                    
                    # Save progress after processing each batch
                    if response.get("NextContinuationToken"):
                        continuation_token = response["NextContinuationToken"]
                        c.execute("""
                            INSERT OR REPLACE INTO continuation_state (id, continuation_token, last_updated) 
                            VALUES (1, ?, CURRENT_TIMESTAMP)
                        """, (continuation_token,))
                        conn.commit()
                        logger.info(f"Continuation token saved.")
                    else:
                        more_objects = False
                        logger.info("Reached end of bucket listing.")
                
                except Exception as e:
                    # If we fail during a batch, save what we have and re-raise
                    logger.error(f"Error during object listing: {str(e)}")
                    if objects_in_current_tx > 0:
                        conn.commit()
                        logger.info(f"Saved progress before error, total processed: {total_objects}")
                    raise
                    
                # Reset cursor periodically
                if objects_processed_since_cursor_reset >= 100000:
                    # Commit any pending changes first
                    if objects_in_current_tx > 0:
                        conn.commit()
                        objects_in_current_tx = 0
                    
                    # Close and recreate cursor
                    logger.debug("Recycling database cursor after 100,000 objects")
                    c.close()
                    c = conn.cursor()
                    objects_processed_since_cursor_reset = 0
                    
        except KeyboardInterrupt:
            logger.warning("Process interrupted. Progress has been saved.")
            conn.commit()
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            conn.commit()
            raise
        finally:
            # Make sure changes are committed
            if objects_in_current_tx > 0:
                conn.commit()
                
            # Update final stats
            c.execute("""
                UPDATE listing_stats 
                SET total_objects = ?, filtered_objects = ?, last_update_time = datetime('now')
                WHERE id = 1
            """, (total_objects, filtered_objects))
            conn.commit()
    
    # Clear the continuation token when complete if we finished
    if not more_objects:
        end_time = time.time()
        duration = end_time - start_time
        
        with sqlite3.connect(db_path) as conn:
            optimize_db_connection(conn)
            c = conn.cursor()
            c.execute("DELETE FROM continuation_state WHERE id = 1")
            
            # Update stats with finished time
            c.execute("""
                UPDATE listing_stats 
                SET finished_time = datetime('now')
                WHERE id = 1
            """)
            conn.commit()
            
        logger.info(f"Listing completed successfully.")
        logger.info(f"Total objects processed: {total_objects}")
        logger.info(f"Objects created before {cutoff_date.strftime('%Y-%m-%d')}: {filtered_objects}")
        logger.info(f"Execution time: {duration:.2f} seconds")
        
        # Create tables for tracking and clean up cos_objects
        cleanup_after_listing(db_path)

################### MAIN EXECUTION ###################
# Entry point and command processing
if __name__ == "__main__":
    # Load environment variables from .env file
    bucket = os.environ.get('SOURCE_BUCKET')
    if not bucket:
        logger.error("Error: SOURCE_BUCKET not defined in .env file")
        exit(1)
    
    print("Script starting...")
    print(f"Using environment variables from: {os.path.abspath('.env')}")
    print(f"Current working directory: {os.getcwd()}")
    
    # Process command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "list":
        # List objects to database
        logger.info("Starting to list objects from bucket to database")
        list_cos_objects_to_sqlite(
            bucket_name=bucket,
            db_path="cos_status.db"
        )
    elif len(sys.argv) > 1 and sys.argv[1] == "stats":
        # Print database stats
        with sqlite3.connect("cos_status.db") as conn:
            c = conn.cursor()
            
            try:
                logger.info("============= DATABASE STATISTICS =============")
                
                c.execute("SELECT COUNT(*) FROM cos_objects")
                result = c.fetchone()
                logger.info(f"Objects remaining to archive: {result[0] if result else 0}")
                
                c.execute("SELECT COUNT(*) FROM copied_keys")
                result = c.fetchone()
                logger.info(f"Objects successfully archived: {result[0] if result else 0}")
                
                c.execute("SELECT COUNT(*) FROM failed_keys")
                result = c.fetchone()
                logger.info(f"Objects failed to archive: {result[0] if result else 0}")
                
                c.execute("SELECT total_success, total_failures, start_time, last_update_time FROM archive_stats WHERE id=1")
                result = c.fetchone()
                if result:
                    # Format dates to be more human-readable if they exist
                    start_time = result[2] if result[2] else "Not started"
                    last_update = result[3] if result[3] else "Never"
                    
                    # Try to parse and format the dates if they're not None
                    if start_time != "Not started":
                        try:
                            # Parse the datetime (assuming ISO format from SQLite)
                            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                            # Format in a more readable way
                            start_time = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            pass  # Keep the original format if parsing fails
                            
                    if last_update != "Never":
                        try:
                            # Parse the datetime (assuming ISO format from SQLite)
                            update_dt = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                            # Format in a more readable way
                            last_update = update_dt.strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            pass  # Keep the original format if parsing fails
                    
                    logger.info(f"Total archive operations - Success: {result[0]}, Failed: {result[1]}")
                    logger.info(f"Archiving started: {start_time}, Last update: {last_update}")
                    
                    # Calculate and display estimated completion time
                    try:
                        # Get remaining objects count
                        c.execute("SELECT COUNT(*) FROM cos_objects")
                        objects_remaining = c.fetchone()[0]
                        
                        # Calculate time elapsed since start
                        if start_time != "Not started" and last_update != "Never":
                            start_dt = datetime.fromisoformat(result[2].replace('Z', '+00:00'))
                            update_dt = datetime.fromisoformat(result[3].replace('Z', '+00:00'))
                            
                            elapsed_seconds = (update_dt - start_dt).total_seconds()
                            
                            if elapsed_seconds > 0 and result[0] > 0:
                                # Calculate processing rate (objects per second)
                                rate = result[0] / elapsed_seconds
                                
                                if rate > 0:
                                    # Estimate remaining time
                                    est_seconds_remaining = objects_remaining / rate
                                    est_days_remaining = est_seconds_remaining / 86400  # seconds in a day
                                    est_hours_remaining = (est_seconds_remaining % 86400) / 3600
                                    
                                    # Calculate estimated completion date
                                    now = datetime.now()
                                    completion_date = now + timedelta(seconds=est_seconds_remaining)
                                    
                                    # Display estimates
                                    logger.info(f"Current processing rate: {rate:.2f} objects/second")
                                    logger.info(f"Estimated remaining time: {est_days_remaining:.1f} days, {est_hours_remaining:.1f} hours")
                                    logger.info(f"Estimated completion date: {completion_date.strftime('%Y-%m-%d %H:%M:%S')}")
                    except Exception as e:
                        # Don't let estimation errors affect the stats command
                        logger.debug(f"Error calculating estimates: {str(e)}")
                
                logger.info("===============================================")
            except Exception as e:
                logger.error(f"Error getting stats: {str(e)}")
    elif len(sys.argv) > 1 and sys.argv[1] == "archive":
        # Check for thread count parameter
        thread_count = 4  # Default
        batch_size = 100  # Default
        
        if len(sys.argv) > 2:
            try:
                thread_count = int(sys.argv[2])
            except ValueError:
                logger.warning(f"Invalid thread count '{sys.argv[2]}'. Using default (4).")
                
        if len(sys.argv) > 3:
            try:
                batch_size = int(sys.argv[3])
            except ValueError:
                logger.warning(f"Invalid batch size '{sys.argv[3]}'. Using default (100).")
        
        logger.info(f"Starting threaded archiving with {thread_count} threads and batch size {batch_size}...")
        archive_objects(
            bucket_name=bucket,
            db_path="cos_status.db",
            batch_size=batch_size,
            max_workers=thread_count
        )
    elif len(sys.argv) > 1 and sys.argv[1] == "list-before":
        cutoff_date = "2025-06-13"  # Default to June 13, 2025
        
        # Allow custom date in format YYYY-MM-DD
        if len(sys.argv) > 2:
            try:
                # Just validate the format
                datetime.strptime(sys.argv[2], "%Y-%m-%d")
                cutoff_date = sys.argv[2]
            except ValueError:
                logger.warning(f"Invalid date format '{sys.argv[2]}'. Using default (2025-06-13).")

        logger.info(f"Starting to list objects created before {cutoff_date} from bucket to database")
        # Pass the date as a string and let the function handle the parsing
        list_cos_objects_to_sqlite_with_date_filter(
            bucket_name=bucket,
            db_path="cos_status.db",
            cutoff_date=cutoff_date
        )
    else:
        # Default action is to archive with continuous processing
        batch_size = 100
        if len(sys.argv) > 1:
            try:
                batch_size = int(sys.argv[1])
            except ValueError:
                logger.warning(f"Warning: Invalid batch size '{sys.argv[1]}'. Using default (100).")
        
        # Check if database exists, if not, warn user
        if not os.path.exists("cos_status.db"):
            logger.error("Database cos_status.db not found. Please run 'python loader.py list' first to create it.")
            exit(1)
        
        logger.info("Starting continuous archiving process...")
        archive_objects(
            bucket_name=bucket,
            db_path="cos_status.db",
            batch_size=batch_size
        )