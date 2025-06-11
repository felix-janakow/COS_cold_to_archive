### Step 1: Download the Script

First create a dedicated folder for the script. Use the following `curl` command to download the script from the GitHub repository:

```bash
curl -O https://raw.githubusercontent.com/felix-janakow/COS_cold_to_archive/main/archive_fbf_non_ineteractive.py
```


### Step 2: create .env file in the same folder with achive_fbf_onon_interactive.py

- You have to manually create a .env file for this script to execute 
- Create the file by typing 

```bash
touch .env
```
- edit the file with the editor of your choice (e.g. vim, gedit, vscode)
- You will need the following variables, if you are unable to locate the varaible values refer to the step by step walktrough in [prerequisites.md](https://github.com/felix-janakow/COS_cold_to_archive/blob/main/Instructions/Prerequisites.md)
``` 
SOURCE_BUCKET=bucket-name
DESTINATION_BUCKET=same as SOURCE_BUCKET 
IAM_API_KEY=abcdefghi123xyz
REGION=eu-de
```
## Running the Script non interactive

- Start the script without creating an additional .output file (recommended since logging is already implemented within the script)

```bash
nohup python3 archive_fbf_non_ineteractive.py > /dev/null 2>&1 &
```

- To just start the script non interactive without further considerations just go with (script will create a .output file whit all of the outpur of the terminal):

```bash
nohup python3 archive_fbf_non_ineteractive.py
```


You will be prompted to enter the variables identified in the **Preparation - Variables** step. Enter the values you noted earlier and start the script.

> [!NOTE]
> Refer to [features+functions.md](https://github.com/felix-janakow/COS_cold_to_archive/blob/main/features%2Bfunctions.md) to learn more about this script and its capabilities.

The script will execute folder by folder and display a progress bar

After execution, the script will have created the following folder structure:

```
folder
├── copied_keys
├── failed_keys
├── logs
├── .env
├── structure.txt
└── archive_fbf_non_interactive.py
```
- **copied_keys** contains all successfully archived files
- **failed_keys** contains all unsuccessfully archived files

***The maximum number of lines per txt file can be adjusted in the control plane using ``MAX_KEYS_PER_FILE``. This is important because very large files (> 1 million lines) can consume a lot of RAM.***

- Logs are written to the **logs** directory

- structure.txt show the exisiting folder/subdolder structure
- after a folder/subfoldr is finished the script will update this txt with the number of archived files next to the folder/subfolder name

