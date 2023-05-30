import json
import urllib.parse
# import boto3
import datetime
import os

print('Loading function')

# s3_client = boto3.client('s3')
# s3_resource = boto3.resource('s3')


def process(event, context):
    """Partitions the logs S3 bucket based on year, month, day, and hour.

        Args:
            event (obj): The event that triggers the lambda run. In this case, a scheduled daily event.

        Raises:
            Exception: Exception for an error during lambda run.

        Returns:
            None.
        """
    print("Received event: " + json.dumps(event, indent=2))

    # connec tto s3 bucket, get all objects from last 30 min and partition - delete old 
    # schedule for every 60 min
    # bucket = s3_resource.Bucket('bgov-logs-dev-us-east-1-319620378945')
    b = 'bgov-logs-dev-us-east-1-319620378945'
    try:
        for file in b.objects.filter(Prefix='logs/s3/bgov-infinity-dev-us-east-1/2', MaxKeys=500):
            key = file.key
            file_name = key.split("/")
            assert len(file_name) == 4
            orig_key = file_name[3]
            partitions = file_name[3].split("-")
            new_key = "logs/s3/bgov-infinity-dev-us-east-1/year="+partitions[0]+"/month="+partitions[1]+"/day="+partitions[2]+"/hour="+partitions[3]+"/"+orig_key

            # s3_resource.Object(b, new_key).copy_from(CopySource=b+"/"+key)
            # s3_resource.Object(b, key).delete()

    except Exception as e:
        print(e)
        print('Error copying/deleting log files from bucket.')
        raise e
