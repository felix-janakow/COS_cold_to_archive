# Tutorial - Python Script Cold Vault -> Archive Tier

This script is used to move data from a COS bucket in the Cold Vault tier to the Archive tier. This is done via a REPLACE operation on the metadata of the objects in the bucket. This operation triggers a change that is recognized by COS and starts the archiving process.

> [!NOTE]  
> The REPLACE operation affects metadata only – the file content itself remains unchanged.  
>  
> Depending on the selected archive type, restoring archived data may take up to 2 hours (Accelerated) or up to 12 hours (with Cold Archive).       

## Preparation

When the script is executed, you will be prompted to provide the following input data: 

    - SOURCE_BUCKET + DESTINATION_BUCKET
    - IAM_API_KEY
    - REGION

These values are stored in a `.env` file and placed locally next to the script.

### Finding the required input data

Since it may not be immediately clear where to find the necessary information, the following explains how to retrieve each variable:


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
