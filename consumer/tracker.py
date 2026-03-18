import json
import redis
from confluent_kafka import Consumer, Producer

# Redis connection
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}
consumer = Consumer(consumer_config)
dlq_producer = Producer({"bootstrap.servers": "localhost:9092"})
consumer.subscribe(["orders"])
MAX_RETRIES = 3
print("Consumer")


def process_order(order):
    print(f"Tracker ...{order['order_id'][-5:]} : Processed {order['quantity']} x {order['item']} for {order['user']}")


while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error:", msg.error())
        continue

    value = msg.value().decode("utf-8")
    # Validate JSON
    try:
        order = json.loads(value)
    except json.JSONDecodeError:
        print("Invalid JSON → DLQ")
        dlq_producer.produce("orders_dlq", value)
        dlq_producer.flush()
        consumer.commit(msg)
        continue

    order_id = order.get("order_id")
    if not order_id:
        print("Missing order_id → DLQ")
        dlq_producer.produce("orders_dlq", value)
        dlq_producer.flush()
        consumer.commit(msg)
        continue

    # Redis deduplication
    if r.exists(f"processed:{order_id}"):
        print(f"Duplicate detected → skip {order_id}")
        consumer.commit(msg)
        continue

    try:
        process_order(order)
        # Mark as processed in Redis
        r.set(f"processed:{order_id}", "1")
        consumer.commit(msg)
    except Exception as e:
        print("Processing failed:", e)
        retry_key = f"retry:{order_id}"
        retries = int(r.get(retry_key) or 0)
        if retries < MAX_RETRIES:
            retries += 1
            r.set(retry_key, retries)
            order["retries"] = retries
            dlq_producer.produce("orders_retry", json.dumps(order))
        else:
            print("Max retries reached → DLQ")
            dlq_producer.produce("orders_dlq", json.dumps(order))

        dlq_producer.flush()
        consumer.commit(msg)
