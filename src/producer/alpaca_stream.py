import argparse
import asyncio
import json
import os
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "market.trades"

LIVE_STREAM_URL = "wss://stream.data.alpaca.markets/v2/iex"
TEST_STREAM_URL = "wss://stream.data.alpaca.markets/v2/test"

LIVE_SYMBOLS = ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN"]
TEST_SYMBOLS = ["FAKEPACA"]

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
)


def get_stream_config(mode):
    if mode == "test":
        return TEST_STREAM_URL, TEST_SYMBOLS, "alpaca_test"

    return LIVE_STREAM_URL, LIVE_SYMBOLS, "alpaca_iex"


def normalize_trade(trade, source):
    return {
        "asset_type": "stock",
        "symbol": trade["S"],
        "price": trade["p"],
        "quantity": trade["s"],
        "exchange": trade.get("x"),
        "event_timestamp": trade["t"],
        "received_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Kafka delivery successful | "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


async def stream_market_data(mode):
    if not API_KEY or not SECRET_KEY:
        raise ValueError("Alpaca API credentials not found in .env")

    stream_url, symbols, source = get_stream_config(mode)

    print(f"Mode: {mode}")
    print(f"Source: {source}")

    async with websockets.connect(stream_url) as websocket:
        connection_response = json.loads(await websocket.recv())
        print("Connection:", connection_response)

        await websocket.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": API_KEY,
                    "secret": SECRET_KEY,
                }
            )
        )

        auth_response = json.loads(await websocket.recv())
        print("Authentication:", auth_response)

        if not any(
            message.get("msg") == "authenticated"
            for message in auth_response
        ):
            raise ConnectionError("Alpaca authentication failed")

        await websocket.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "trades": symbols,
                }
            )
        )

        subscription_response = json.loads(await websocket.recv())
        print("Subscription:", subscription_response)

        print(f"\nStreaming trades for {', '.join(symbols)}...")
        print(f"Publishing to Kafka topic: {KAFKA_TOPIC}\n")

        while True:
            messages = json.loads(await websocket.recv())

            for message in messages:
                if message.get("T") != "t":
                    continue

                event = normalize_trade(message, source)

                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=event["symbol"],
                    value=json.dumps(event),
                    callback=delivery_report,
                )

                producer.poll(0)

                print(
                    f"Trade | "
                    f"{event['symbol']} "
                    f"price={event['price']} "
                    f"quantity={event['quantity']}"
                )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream Alpaca market trades into Kafka."
    )

    parser.add_argument(
        "--mode",
        choices=["test", "live"],
        default="test",
        help="Use Alpaca's test stream or live IEX stream.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    try:
        asyncio.run(stream_market_data(args.mode))

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()