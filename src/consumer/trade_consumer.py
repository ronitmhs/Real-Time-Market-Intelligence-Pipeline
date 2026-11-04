import json
import os

import psycopg
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "market.trades"
CONSUMER_GROUP = "market-trade-consumers"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "market_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "market_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "market_password")


consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",

        # We will commit only after successful database processing.
        "enable.auto.commit": False,
    }
)


def get_db_connection():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def insert_trade(connection, event, message):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO market_trades (
                asset_type,
                symbol,
                price,
                quantity,
                exchange,
                event_timestamp,
                received_timestamp,
                source,
                kafka_topic,
                kafka_partition,
                kafka_offset
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (
                kafka_topic,
                kafka_partition,
                kafka_offset
            )
            DO NOTHING
            """,
            (
                event["asset_type"],
                event["symbol"],
                event["price"],
                event["quantity"],
                event.get("exchange"),
                event["event_timestamp"],
                event["received_timestamp"],
                event["source"],
                message.topic(),
                message.partition(),
                message.offset(),
            ),
        )

    connection.commit()


def consume_trades():
    connection = get_db_connection()

    print("Connected to PostgreSQL.")

    consumer.subscribe([KAFKA_TOPIC])

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    print(f"Consumer group: {CONSUMER_GROUP}\n")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"Kafka error: {message.error()}")
                continue

            try:
                event = json.loads(
                    message.value().decode("utf-8")
                )

                insert_trade(connection, event, message)

                # Commit Kafka offset only after PostgreSQL succeeds.
                consumer.commit(message=message, asynchronous=False)

                print(
                    f"Stored event | "
                    f"symbol={event['symbol']} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )

            except Exception as error:
                connection.rollback()

                print(
                    f"Failed to process offset "
                    f"{message.offset()}: {error}"
                )

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    consume_trades()
