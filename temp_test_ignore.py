import boto3
import json
import time
from datetime import datetime
for i in range(10):
    if i%2 ==0:
        product_id=1
    else:
        product_id=2
    record = {
        "event_time": datetime.utcnow().isoformat() + "Z",
        "product_id": product_id,
        "item_id": f"item_{1000 + i}",
        "price": 100 + i*10
    }
    time.sleep(1)

    print(record)