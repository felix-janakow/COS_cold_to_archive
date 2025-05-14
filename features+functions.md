
## Script Features and Functions

### Control Plane

- **Centralized Configuration**: All configurable parameters are centralized, making it easy to adjust settings without modifying the core logic.
- **Key Parameters**:
  - `MAX_KEYS_PER_FILE`: Maximum number of lines per key file before rotating.
  - `BATCH_SIZE`: Number of objects processed per batch.
  - `MAX_RETRIES`: Maximum number of retries for copy operations.
  - `BACKOFF_FACTOR`: Exponential backoff factor for retry logic.
  - `THROTTLE_DELAY`: Delay between API calls to prevent throttling.
  - `USE_EMOJIS`: Toggle for emoji output in logs.

### Directory Setup

- **Automatic Directory Creation**: Creates directories for storing copied keys, failed keys, and logs.
  - `COPIED_KEYS_DIR`: Directory for successfully copied keys.
  - `FAILED_KEYS_DIR`: Directory for keys that failed to copy.
  - `LOG_DIR`: Directory for log files.

### Input Handling

- **User Input Collection**: Collects necessary input from the user and saves it to a `.env` file.
  - `BUCKET`: Used for both source and destination.
  - `IAM_API_KEY`: API key for authentication.
  - `REGION`: Region of the COS instance.

### Logging

- **Detailed Logging**: Provides detailed information about the script's execution, including timestamps, log levels, and messages.

### Emoji Support

- **User-Friendly Logging**: Supports emoji output for a more user-friendly logging experience, indicating success, errors, retries, and informational messages.

### Retry Logic with Exponential Backoff

- **Robustness and Reliability**: Includes a retry mechanism with exponential backoff to handle transient errors during copy operations.

### Throttling

- **API Call Management**: Includes a delay between API calls to prevent throttling.

### Key Handling with File Rotation

- **Efficient Key Management**: Manages keys with file rotation to handle large numbers of objects efficiently. Loads, saves, and rotates key files based on the configured maximum lines per file.

### Batch Processing

- **Optimized Performance**: Processes objects in batches to optimize performance and manage large datasets efficiently.
  - `process_batch`: Handles the copying of objects in a batch, with retry logic for robustness.
  - `copy_objects_in_batches`: Manages the overall batch processing, including pagination through the bucket's objects.

### Retry Mechanism for Failed Keys

- **Ensures Completion**: Includes a mechanism to retry copying failed keys, ensuring transient errors do not prevent the archiving process from completing.
  - `retry_failed_keys`: Attempts to copy objects that previously failed.
  - `remove_key_from_failed_keys`: Removes successfully copied keys from the list of failed keys.

### Progress Tracking

- **Real-Time Updates**: Uses `tqdm` to provide real-time progress updates in the terminal, including total files to process, batch processing progress, retry progress, and final summary.

### Execution Modes
- **Flexible Execution**: Supports two execution modes:
  - Normal mode: Processes objects in batches.
  - Retry mode: Retries copying objects that previously failed.
