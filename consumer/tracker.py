import json
from confluent_kafka import Consumer, Producer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

consumer = Consumer(consumer_config)
dlq_producer = Producer({"bootstrap.servers": "localhost:9092"})
consumer.subscribe(["orders"])

processed_ids = set()
MAX_RETRIES = 3

print("Consumer is running and subscribed to orders topic")

def process_order(order):
    # Your business logic here
    print(f"Processed {order['quantity']} x {order['item']} for {order['user']}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        value = msg.value().decode("utf-8")

        # Handle invalid JSON
        try:
            order = json.loads(value)
        except json.JSONDecodeError:
            print("Invalid JSON, sending to DLQ:", value)
            dlq_producer.produce("orders_dlq", value)
            dlq_producer.flush()
            consumer.commit(msg)
            continue

        order_id = order.get("order_id")
        if not order_id:
            print("Missing order_id, sending to DLQ")
            dlq_producer.produce("orders_dlq", value)
            dlq_producer.flush()
            consumer.commit(msg)
            continue

        # Deduplication
        if order_id in processed_ids:
            print(f"Duplicate detected, skipping {order_id}")
            consumer.commit(msg)
            continue

        try:
            process_order(order)
            processed_ids.add(order_id)
            consumer.commit(msg)

        except Exception as e:
            print("Processing failed:", e)
            retries = order.get("retries", 0)

            if retries < MAX_RETRIES:
                order["retries"] = retries + 1
                dlq_producer.produce("orders_retry", json.dumps(order))
            else:
                dlq_producer.produce("orders_dlq", json.dumps(order))

            dlq_producer.flush()
            consumer.commit(msg)

except KeyboardInterrupt:
    print("\nStopping consumer")

finally:
    consumer.close()
