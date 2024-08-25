# Overview

This AWS Lambda function automates the process of monitoring a specific S3 bucket for CSV uploads, extracting incomplete tasks from the CSV, summarizing the data, and sending out a formatted HTML email to designated recipients. The function leverages AWS services such as S3, SES (Simple Email Service), and Secrets Manager.

## Functionality

	1.	Trigger and Event Handling:
	•	The Lambda function is triggered by the s3:ObjectCreated:* event, meaning it executes whenever a new object (CSV file) is uploaded to the designated S3 bucket.
	2.	Fetching and Processing Data:
	•	Upon triggering, the function retrieves the uploaded CSV file from S3.
	•	It reads the CSV content into a Pandas DataFrame and filters rows where the complete column is set to False, indicating incomplete tasks.
	3.	Data Aggregation:
	•	The function groups the incomplete tasks by their type and counts the number of occurrences of each type.
	•	The grouped data is converted into a dictionary format for easy HTML table generation.
	4.	Email Notification:
	•	The Lambda function sends an email via AWS SES to a list of recipients. The email contains a summary of the incomplete tasks and an HTML-formatted table showcasing the number of incomplete actions by type.
	•	The HTML content is dynamically generated, incorporating the current week number and a button for recipients to update the status of tasks on a specified portal.
	5.	Secrets Management:
	•	AWS Secrets Manager is used to securely store sensitive information such as SES ARN and recipient email addresses.
	•	The function retrieves these secrets at runtime to configure SES and determine email recipients.

## IAM Roles and Permissions

To deploy and run this Lambda function successfully, the following IAM roles and permissions are required:

	1.	Lambda Execution Role:
	•	s3:GetObject: Required to read the CSV files from the S3 bucket.
	•	ses:SendEmail: Required to send emails via AWS SES.
	•	secretsmanager:GetSecretValue: Required to retrieve secrets from AWS Secrets Manager.
	•	logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents: Required to create and write logs to CloudWatch for debugging and monitoring.
	2.	S3 Bucket Policy:
	•	Grants the Lambda function permission to read objects in the S3 bucket.