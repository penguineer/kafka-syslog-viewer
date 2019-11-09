#!/usr/bin/python3

import signal
import sys
import argparse
from datetime import datetime

import json

import kafka

from termcolor import colored


def sigint_handler(_signal, _frame):
    print("SIGINT received. Exit.")
    sys.exit(0)


def print_syslog_entry(offset, entry):
    msgcolor = 'white' if entry['severity'] in ['notice', 'info', 'debug'] else 'red'

    dt = datetime.strptime(entry['timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z')

    print("{date} {time} {host:32} {lvl:12} {fac}".format(
        ofs=colored(offset, 'white'),
        date=colored(datetime.strftime(dt, '%Y-%m-%d'), 'white'),
        time=colored(datetime.strftime(dt, '%H:%M:%S'), 'white', attrs=['bold']),
        host=colored(entry['host'], 'yellow'),
        lvl=colored(entry['severity'], msgcolor),
        fac=colored(entry['facility'], 'white')
    ))

    print("{0} {1}".format(colored(entry['syslog-tag'], msgcolor),
                           colored(entry['message'], msgcolor, attrs=['bold'])))

    print()


def follow_syslog(consumer):
    for entry in consumer:
        print_syslog_entry(entry.offset, entry.value)


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
