import boto3
import json
import time
from datetime import datetime


session = boto3.Session()
kinesis = session.client('kinesis')


stream_name = 'aws_demo_stream'


for i in range(10):
    if i%2 ==0:
        product_id='Product1'
    else:
        product_id='Product2'
    record = {
        "event_time": datetime.utcnow().isoformat() + "Z",
        "product_id": product_id,
        "item_id": f"item_{1000 + i}",
        "price": 100 + i*10
    }
    time.sleep(2)

    print(record)

    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(record),
        PartitionKey=record["product_id"]
    )

    print(f"Sent record {i+1}: {record}")
    time.sleep(1)  # Simulate events
