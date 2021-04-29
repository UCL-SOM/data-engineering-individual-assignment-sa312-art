#lambda code to process tweets automatically from S3 bucket to AWS Dynamodb
import json
import csv 
import boto3
def lambda_handler(event, context):
    region = 'eu-west-1'
    record_ddlist = [] 
    try:
        s3 = boto3.client('s3') 
        dynamodb = boto3.client('dynamodb', region_name = region)
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print('Bucket: ', bucket, 'Key: ', key)
        csv_file = s3.get_object(Bucket=bucket, Key=key)
        record_list = csv_file["Body"].read().decode('utf-8').split('\n')
        csv_reader = csv.reader(record_list, delimiter=',', quotechar='"')
        for row in csv_reader:
            id = row[1] 
            text = row[2] 
            label = row[3]
            add_to_db = dynamodb.put_item(
                TableName = 'covid_tweets',
                Item = {
                    'id' : {'S': str(id)},
                    'label' : {'S': str(label)},
                    'text' : {'S': str(text)},
                    })
            print("Successfully added")
    except Exception as e:
        print(str(e))