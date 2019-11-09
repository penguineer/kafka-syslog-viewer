
import signal
import sys
import argparse
import time

import json

import kafka


def sigint_handler(_signal, _frame):
    print("SIGINT received. Exit.")
    sys.exit(0)


def print_syslog_entry(entry):
    print(entry)


def follow_syslog(consumer):
    for entry in consumer:
        print_syslog_entry(entry)


def main():
    parser = argparse.ArgumentParser(
        description="Gatekeeper Door Service")
    parser.add_argument("--bootstrap", help="Kafka bootstrap server", default="localhost")
    parser.add_argument("--topic", help="Kafka topic", default="syslog")
    args = parser.parse_args()

    consumer = kafka.KafkaConsumer(args.topic,
                                   bootstrap_servers=[args.bootstrap],
                                   auto_offset_reset='earliest',
                                   value_deserializer=lambda x: json.loads(x.decode('utf-8'), strict=False))

    follow_syslog(consumer)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)

    main()
