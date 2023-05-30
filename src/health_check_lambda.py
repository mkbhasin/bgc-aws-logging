import json
import urllib.parse
# import boto3
import os
from datetime import datetime
from datetime import timedelta

print('Loading function')

# s3_client = boto3.client('s3')
# s3_resource = boto3.resource('s3')


def process(event, context):
    """Checks the content of bgov-infinity S3 bucket and verifies against size and count thresholds for each DL table.

        Args:
            event (obj): The event that triggers the lambda run. In this case, a scheduled weekly event.

        Raises:
            Exception: Exception for a file that does not meet count or size threshold.

        Returns:
            None.
        """
    print("Received event: " + json.dumps(event, indent=2))

    # connect to s3 bucket, verify against thresholds - size / number of files 
    # bucket = s3_resource.Bucket('bgov-infinity-dev-us-east-1')
    b = "bucket"
    folders = {'agency/':[5, 6.5], 'bgov_market/':[100, 20], 'defense_budget/':[5, 18], 'dibbs/':[100, 6.5], 'fusion/':[150, 200], 
    'incumbent_award/':[5, 8.5], 'it_budget_cy/':[5, 0.5], 'it_budget_details/':[5, 5], 'it_budget_transaction/':[5, .016], 
    'it_budget/':[5, .6], 'notice_document/':[20, 17.5], 'solicitation/':[50, 120], 'subaward/':[100, 9], 
    'transaction/':[300, 150], 'vendor_bbid/':[5, .995], 'vendor/':[5, 20]}
    today_date = (datetime.utcnow() - timedelta(days = 1)).strftime('%Y-%m-%d')
    try:
        for (key, value) in folders.items():
            count = 0
            for obj in b.objects.filter(Prefix='bgc/data/csv/govcon/'+key+today_date):
                count += 1
                assert bytesto(obj.size, 'm', 1024) >= value[1]
                if count > value[0]:
                    break
    except Exception as e:
        print(e)
        print('File count and/or size threshold not met for govcon csv file.')
        raise e

    
    # connect to s3 bucket, verify against thresholds - size / number of files 
    folders = {'agency/':[5, 3], 'bgov_market/':[100, 3], 'defense_budget/':[5, 7], 'dibbs/':[100, 2], 'fusion/':[15, 450], 
    'incumbent_award/':[5, 3], 'it_budget_cy/':[5, 0.2], 'it_budget_details/':[5, 2], 'it_budget_transaction/':[5, .011], 
    'it_budget/':[5, .25], 'notice_document/':[20, 8], 'solicitation/':[50, 45], 'subaward/':[100, 2], 
    'transaction/':[300, 25], 'vendor/':[5, 10]}
    try:
        for (key, value) in folders.items():
            count = 0
            for obj in b.objects.filter(Prefix='bgc/govcon-analytics/data/'+key):
                count += 1
                assert bytesto(obj.size, 'm', 1024) >= value[1]
                if count > value[0]:
                    break
    except Exception as e:
        print(e)
        print('File count and/or size threshold not met for govcon parquet file.')
        raise e

def bytesto(bytes, to, bsize=1024):
    """Transforms bytes to specified size.

        Args:
            bytes (int): The input size of bytes.
            to (string): The output size format (k=kilobyte, m=megabyte).
            bsize (int, optional): Number of bytes in a kilobytes. Defaults to 1024.

        Returns:
            float: The result of the conversion from bytes to required output.
        """
    a = {'k' : 1, 'm': 2}
    r = float(bytes)
    return bytes / (bsize ** a[to])