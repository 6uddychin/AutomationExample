import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')
secrets_manager = boto3.client('secretsmanager')
ses = boto3.client('ses')

# Retrieve the necessary secrets from Secrets Manager
def get_secret_value(secret_name):
    response = secrets_manager.get_secret_value(SecretId=secret_name)
    return response['SecretString']

# Retrieve SES ARN and bucket ARN from Secrets Manager
SES_ARN = get_secret_value("SES_Arn")
INPUT_S3_BUCKET_ARN = get_secret_value("input_s3_bucket_arn")
ELE_EMAIL = get_secret_value("ele_email")
MER_EMAIL = get_secret_value("mer_email")

def lambda_handler(event, context):
    bucket_name = INPUT_S3_BUCKET_ARN.split(':')[-1]  # Extract bucket name from ARN
    object_key = event['Records'][0]['s3']['object']['key']
    
    week_number = datetime.utcnow().isocalendar()[1]
    new_key = f"{week_number}_requested.csv"
    
    if check_file_exists(bucket_name, new_key):
        completed_key = f"{week_number}_completed.csv"
        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': object_key}, Key=completed_key)
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        
        # Download both requested and completed CSVs
        requested_csv = s3.get_object(Bucket=bucket_name, Key=new_key)['Body'].read().decode('utf-8')
        completed_csv = s3.get_object(Bucket=bucket_name, Key=completed_key)['Body'].read().decode('utf-8')
        
        # Convert to DataFrames
        requested_df = pd.read_csv(StringIO(requested_csv))
        completed_df = pd.read_csv(StringIO(completed_csv))
        
        # Find incomplete actions
        incomplete_df = requested_df[~requested_df.isin(completed_df.to_dict('l')).all(axis=1)]
        
        # Group by owner and send emails
        if not incomplete_df.empty:
            grouped = incomplete_df.groupby('owner')
            for owner, group in grouped:
                owner_csv = group.to_csv(index=False)
                owner_key = f"{week_number}_{owner}_incomplete.csv"
                
                s3.put_object(Bucket=bucket_name, Key=owner_key, Body=owner_csv)
                
                # Get the corresponding email from Secrets Manager based on the owner
                if owner.lower() == 'ele':
                    owner_contact = ELE_EMAIL
                elif owner.lower() == 'merc':
                    owner_contact = MER_EMAIL
                else:
                    continue  # Skip if the owner is not recognized
                
                # Send the email with the CSV attachment
                send_email(owner_contact, owner_key, owner_csv, bucket_name, len(group))
                
        return {
            'statusCode': 200,
            'body': 'Successfully processed and emailed incomplete actions CSV files.'
        }
    else:
        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': object_key}, Key=new_key)
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        
        # Continue with your existing logic to process and send the requested CSVs
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
        
        return {
            'statusCode': 200,
            'body': 'Successfully processed and emailed requested CSV files.'
        }

def check_file_exists(bucket_name, key):
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

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

    ses.send_email(
        Source=SES_ARN,
        Destination={'ToAddresses': [email]},
        Message={
            'Subject': {'Data': f'CSV Report {csv_key}'},
            'Body': {
                'Html': {'Data': html_content},
                'Text': {'Data': f"There are {row_count} actions that need to be completed this week.\n\nBest regards,\n\nContact your representative with any questions."}
            },
        }
    )