import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from botocore.exceptions import ClientError
import json
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
ses = boto3.client('ses')

def get_secret_value(secret_name, region_name="us-east-1"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)  # Assuming the secret is stored as JSON
    except ClientError as e:
        logger.error(f"Unable to retrieve secret {secret_name}: {e}")
        raise e

# Fetching secrets from AWS Secrets Manager
try:
    secrets = get_secret_value("email_automation_secrets")
except ClientError as e:
    logger.error("Failed to load secrets from Secrets Manager")
    raise e

SES_ARN = secrets["SES_Arn"]
INPUT_S3_BUCKET_ARN = secrets["input_s3_bucket_arn"]
ELE_EMAIL = secrets["ele_email"]
MER_EMAIL = secrets["mer_email"]

def lambda_handler(event, context):
    bucket_name = INPUT_S3_BUCKET_ARN.split(':')[-1]  # Extract bucket name from ARN
    object_key = event['Records'][0]['s3']['object']['key']

    # Log bucket name and object key for debugging
    logger.info(f"Bucket Name: {bucket_name}")
    logger.info(f"Object Key: {object_key}")

    week_number = datetime.utcnow().isocalendar()[1]
    new_key = f"{week_number}_requested.csv"

    # Check if the source object exists
    # if check_file_exists(bucket_name, object_key):
    #     # Only proceed to check for new_key if the source object exists
    #     if check_file_exists(bucket_name, new_key):
    #         completed_key = f"{week_number}_completed.csv"
    #         try:
    #             # Check if the completed_key already exists, if not this should be caught and logged
    #             if check_file_exists(bucket_name, completed_key):
    #                 logger.info(f"{completed_key} already exists in {bucket_name}")
    #             else:
    #                 logger.info(f"{completed_key} does not exist in {bucket_name}, proceeding to copy.")
    #             # Proceed with the copy operation
    #             s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': object_key}, Key=completed_key)
    #             s3.delete_object(Bucket=bucket_name, Key=object_key)
    #         except ClientError as e:
    #             logger.error(f"Failed to copy {object_key} to {completed_key}: {e}")
    #             return {
    #                 'statusCode': 500,
    #                 'body': f"Error copying file: {e}"
    #             }

    requested_csv = s3.get_object(Bucket=bucket_name, Key=new_key)['Body'].read().decode('utf-8')
    completed_csv = s3.get_object(Bucket=bucket_name, Key=completed_key)['Body'].read().decode('utf-8')

    requested_df = pd.read_csv(StringIO(requested_csv))
    completed_df = pd.read_csv(StringIO(completed_csv))

    incomplete_df = requested_df[~requested_df.isin(completed_df.to_dict('l')).all(axis=1)]

    if not incomplete_df.empty:
        grouped = incomplete_df.groupby('owner')
        for owner, group in grouped:
            owner_csv = group.to_csv(index=False)
            owner_key = f"{week_number}_{owner}_incomplete.csv"

            s3.put_object(Bucket=bucket_name, Key=owner_key, Body=owner_csv)

            if owner.lower() == 'ele':
                owner_contact = ELE_EMAIL
            elif owner.lower() == 'merc':
                owner_contact = MER_EMAIL
            else:
                continue  # Skip if the owner is not recognized

            send_email(owner_contact, owner_key, owner_csv, bucket_name, len(group))

        return {
            'statusCode': 200,
            'body': 'Successfully processed and emailed incomplete actions CSV files.'
        }
    # else:
    s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': object_key}, Key=new_key)
    s3.delete_object(Bucket=bucket_name, Key=object_key)

    response = s3.get_object(Bucket=bucket_name, Key=new_key)
    csv_content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_content))
    ele_dataframe = df[df['owner'].str.contains("ele", case=False, na=False)]
    merc_dataframe = df[df['owner'].str.contains("merc", case=False, na=False)]

    ele_row_count = len(ele_dataframe)
    merc_row_count = len(merc_dataframe)

    ele_csv = ele_dataframe.to_csv(index=False)
    merc_csv = merc_dataframe.to_csv(index=False)

    ele_csv_key = f"{week_number}_ele_report.csv"
    merc_csv_key = f"{week_number}_merc_report.csv"

    s3.put_object(Bucket=bucket_name, Key=ele_csv_key, Body=ele_csv)
    s3.put_object(Bucket=bucket_name, Key=merc_csv_key, Body=merc_csv)

    send_email(ELE_EMAIL, ele_csv_key, ele_csv, bucket_name, ele_row_count)
    send_email(MER_EMAIL, merc_csv_key, merc_csv, bucket_name, merc_row_count)
    s3.delete_object(Bucket=bucket_name, Key=object_key)


    return {
        'statusCode': 200,
        'body': 'Successfully processed and emailed requested CSV files.'
    }


def send_email(email, csv_key, csv_content, bucket_name, row_count):
    html_content = f"""
        <html>
            <body>
                <p>There are {row_count} actions that need to be completed this week.</p>
                <p>Best regards,</p>
                <p>Contact your representative with any questions.</p>
            </body>
        </html>
    """

    source_email = SES_ARN.split(':')[-1]  # Extract the email from SES_ARN if it's in ARN format

    try:
        ses.send_email(
            Source=source_email,
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': f'CSV Report {csv_key}'},
                'Body': {
                    'Html': {'Data': html_content},
                    'Text': {'Data': f"There are {row_count} actions that need to be completed this week.\n\nBest regards,\n\nContact your representative with any questions."}
                },
            }
        )
    except ClientError as e:
        logger.error(f"Failed to send email to {email}: {e}")
        raise e
# else:
#     logger.error(f"Object {object_key} does not exist in bucket {bucket_name}.")
#     return {
#         'statusCode': 404,
#         'body': f"Object {object_key} does not exist in bucket {bucket_name}."
#     }

# def check_file_exists(bucket_name, key):
#     try:
#         s3.head_object(Bucket=bucket_name, Key=key)
#         return True
#     except ClientError as e:
#         if e.response['Error']['Code'] == "404":
#             logger.error(f"Object {key} does not exist in bucket {bucket_name}.")
#         else:
#             logger.error(f"Failed to check if {key} exists in {bucket_name}: {e}")
#         return False