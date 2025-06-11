> [!WARNING]
> Before the script is executed, the archiving rule must be defined within the COS Bucket

## Preparation - Variables

When the script is executed, you will be prompted to provide the following input data: 

    - SOURCE_BUCKET + DESTINATION_BUCKET (required)
    - IAM_API_KEY (required)
    - REGION (required)
    - KEYPROTECT_CRN (optional)
    - PREFIX (optional)

These values are stored in a `.env` file and placed locally next to the script.

### Finding the required input data

Since it may not be immediately clear where to find the necessary information, the following explains how to retrieve the ***required variables**:


### - SOURCE_BUCKET and - DESTINATION_BUCKET

-> These two entries are considered together since we are performing an "INPLACE COPY". Source and destination are the same bucket.

- Click on ``Resource List`` on the left-hand side and search for your COS instance containing the buckets you wish to archive.   

![Image](https://github.com/user-attachments/assets/e9d100d3-4c2b-46c9-b002-f847af128bff)

- Copy the name of the bucket and save it in a retrievable format as a note. 

![Image](https://github.com/user-attachments/assets/23996861-7842-4aed-b5f1-704632c81da7)


### - IAM_API_KEY

Create an IAM key by:
- Clicking ``Manage`` in the top navigation and selecting ``Access (IAM)``
- Selecting ``API keys`` in the left sidebar
- Clicking ``Create +``  
- Giving it any name you like
- You can ignore Leaked Action
- For Session Creation, select ``Yes``
- Save the key in a retrievable format as a note

![Image](https://github.com/user-attachments/assets/19934ff3-fce4-4bc5-9059-e0440abaa38b)

### - Region

In our case, the region value is ``eu-de``

## Preparation - Setting up the environment to execute the script

> [!NOTE]
> This example refers to a Linux environment; if you are using a different OS, you will need to follow the requirements for your specific OS

### Step 1: Installing Python 3 and required packages on Linux

To run the script, you need to have Python 3 installed along with several packages. Follow these steps to set up your environment:


```bash
sudo apt update
sudo apt install python3
sudo apt install python3-pip
``` 

### Step 2: Install required packages

Once Python 3 is installed, you need to install the required packages. Run the following commands:
```bash
pip3 install ibm-cos-sdk
pip3 install python-dotenv
pip3 install tqdm
```