import json
import boto3

s3_client = boto3.client('s3')
BUCKET_NAME = 'your-s3-bucket-name' #s3-buke name.....

def lambda_handler(event, context):
    transformed_data = []

    for record in event['Records']:
 
        payload = json.loads(record['kinesis']['data'])
        print(f"Received payload: {payload}")

        payload['geo_location'] = f"Lat-{payload['sensor_id']},Lon-{payload['sensor_id']}"
        transformed_data.append(payload)

    filename = f"transformed_data_{int(time.time())}.json"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=json.dumps(transformed_data)
    )
    print(f"Saved transformed data to S3: {filename}")
