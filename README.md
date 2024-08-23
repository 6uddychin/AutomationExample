# AWS Lambda CSV Processing Automation

This repository contains the AWS Lambda function and associated CloudFormation template for automating the processing of CSV files uploaded to an S3 bucket. The function filters the data, identifies incomplete actions, and sends email notifications to the respective owners using AWS SES.

## Features

- **CSV Processing**: Automatically processes CSV files uploaded to an S3 bucket.
- **Incomplete Actions Tracking**: Compares requested and completed actions to identify incomplete tasks.
- **HTML Email Notifications**: Sends HTML-formatted emails via AWS SES with details about the actions required.
- **AWS Secrets Manager Integration**: Retrieves email addresses from AWS Secrets Manager for sending notifications.

## Prerequisites

Before deploying this solution, ensure you have:

- An AWS account with the necessary permissions to create S3 buckets, Lambda functions, SES identities, and Secrets Manager secrets.
- Python 3.9 or later installed for Lambda runtime.
- AWS CLI installed and configured.

## Deployment

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name