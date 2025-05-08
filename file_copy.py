from ibm_boto3 import client
from ibm_botocore.client import Config
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_botocore.session import Session
import os
from dotenv import load_dotenv

load_dotenv()

def copy_objects(source_bucket, destination_bucket, max_keys=100000000):
    # Set up IBM Cloud Object Storage resource provider
    authenticator = IAMAuthenticator(os.environ['IAM_API_KEY'])
    s3 = client(
        's3',
        ibm_api_key_id=os.environ['IAM_API_KEY'],
        config=Config(signature_version='oauth'),
        endpoint_url=f"https://s3.{os.environ['REGION']}.cloud-object-storage.appdomain.cloud"
    )

    # List all objects in the source bucket
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=source_bucket, MaxKeys=max_keys):
        for obj in page.get('Contents', []):
            # Copy each object to the destination bucket
            copy_source = {
                'Bucket': source_bucket,
                'Key': obj['Key']
            }
            try:
                s3.copy_object(
                    CopySource=copy_source,
                    Bucket=destination_bucket,
                    Key=obj['Key'],
                    MetadataDirective="REPLACE"  
                )
                print(f"✅ {obj['Key']} erfolgreich kopiert.")
                

            except Exception as e:
                print(f"❌ Fehler beim Kopieren von {obj['Key']}: {e}")

if __name__ == '__main__':
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    copy_objects(source_bucket, destination_bucket)


