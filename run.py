from os import path as os_path
import argparse
import configparser
import logging
from multiprocessing import Process

from listeners import WORKER_TYPE_LIST
from worker import etl_event_worker_process

# default config file
ROOT = os_path.dirname(__file__)
jroot = lambda *parts: os_path.join(ROOT, *parts)
DEFAULT_CONFIG_PATH = jroot('config.toml')


def main():
    parser = argparse.ArgumentParser(
        description='Service for work with ESB.'
    )
    parser.add_argument(
        'config', help='Config path', default=DEFAULT_CONFIG_PATH
    )

    args = parser.parse_args()
    config = configparser.RawConfigParser()
    config.read(args.config)

    logging.basicConfig(
        filename=config["LOG"].get("filename", None),
        format=config["LOG"]["format"],
        level=logging.getLevelName(config["LOG"]["level"]),
    )
    for worker_type in WORKER_TYPE_LIST:
        for num in range(int(config["PROCESS_NUM"].get("count", 1))):
            Process(
                target=etl_event_worker_process,
                args=(
                    config["RABBIT_MQ"],
                    config["RABBIT_MQ_WEBSOCKET"],
                    config["DB"],
                    config["REDIS"],
                    num,
                    worker_type,
                )
            ).start()


if __name__ == "__main__":
    main()
