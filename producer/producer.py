import uuid
import json
from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence":True,
    "acks": "all",
    "retries": 5,
    "delivery.timeout.ms": 30000
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f" Delivery failed: {err}")
    else:
        print(f" Delivered: {msg.value().decode('utf-8')}")
        print(f"Delivered to {msg.topic()} | partition : {msg.partition()} | at offset : {msg.offset()} | error : {msg.error()} ")
        print (dir(msg))


order = {
    "order_id": str(uuid.uuid4()),
    "user": "tango",
    "item": "briyani",
    "quantity": 3,

}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    key=order["order_id"],
    value=value,
    callback=delivery_report
)


producer.flush()
