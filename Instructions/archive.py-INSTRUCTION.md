### Step 1: Download the Script

First create a dedicated folder for the script. Use the following `curl` command to download the script from the GitHub repository:

```bash
curl -O https://raw.githubusercontent.com/felix-janakow/COS_cold_to_archive/main/archive.py
```

## Running the Script

To run the script, use the following command:

```bash
python3 archive.py
```

You will be prompted to enter the variables identified in the **Preparation - Variables** step. Enter the values you noted earlier and start the script.

> [!NOTE]
> Refer to [features+functions.md](https://github.com/felix-janakow/COS_cold_to_archive/blob/main/features%2Bfunctions.md) to learn more about this script and its capabilities.

The script will execute and display a progress bar

After execution, the script will have created the following folder structure:

```
folder
├── copied_keys
├── failed_keys
├── logs
├── .env
└── archive_extended.py
```
- **copied_keys** contains all successfully archived files
- **failed_keys** contains all unsuccessfully archived files

***The maximum number of lines per txt file can be adjusted in the control plane using ``MAX_KEYS_PER_FILE``. This is important because very large files (> 1 million lines) can consume a lot of RAM.***

- Logs are written to the **logs** directory
