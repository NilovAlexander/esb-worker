"""
Файл создает бесконечный тестовый поток в rabbit.
"""
import configparser
import json

import stomp
import logging


if __name__ == "__main__":
    config = configparser.RawConfigParser()
    config.read("../conf/etl_events_test.conf")

    logging.basicConfig(
        filename=config["LOG"].get("filename", None),
        format=config["LOG"]["format"],
        level=logging.getLevelName(config["LOG"]["level"]),
    )

    conn = stomp.Connection([(config["RABBIT_MQ"]["host"], int(config["RABBIT_MQ"]["port"]))])
    conn.start()
    conn.connect(config["RABBIT_MQ"]["user"], config["RABBIT_MQ"]["password"], wait=True)

    data = [
        {"value": "True", "tag": "connected", "receive_timestamp": 1498463432.3240337,
         "device_timestamp": 1498463432.3240337, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335, "type": "test"},
        {"value": "1", "tag": "contactor.evening.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431387.0, "controller_id": 335, "user_id": "user"},
        {"value": "1", "tag": "contactor.night.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431382.0, "controller_id": 335, "user_id": "user", "type": "test"},
        {"value": "True", "tag": "connected", "receive_timestamp": 1498463432.3240337,
         "device_timestamp": 1498463432.3240337, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335, "type": "test"},
        {"value": "1", "tag": "contactor.evening.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431387.0, "controller_id": 335, "user_id": "user"},
        {"value": "1", "tag": "contactor.night.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431382.0, "controller_id": 335, "user_id": "user", "type": "test"},
        {"value": "True", "tag": "connected", "receive_timestamp": 1498463432.3240337,
         "device_timestamp": 1498463432.3240337, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335, "type": "test"},
        {"value": "1", "tag": "contactor.evening.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431387.0, "controller_id": 335, "user_id": "user"},
        {"value": "1", "tag": "contactor.night.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431382.0, "controller_id": 335, "user_id": "user", "type": "test"},
        {"value": "True", "tag": "connected", "receive_timestamp": 1498463432.3240337,
         "device_timestamp": 1498463432.3240337, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335, "type": "test"},
        {"value": "1", "tag": "contactor.evening.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431387.0, "controller_id": 335, "user_id": "user"},
        {"value": "1", "tag": "contactor.night.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431382.0, "controller_id": 335, "user_id": "user", "type": "test"},
        {"value": "True", "tag": "connected", "receive_timestamp": 1498463432.3240337,
         "device_timestamp": 1498463432.3240337, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335, "type": "test"},
        {"value": "1", "tag": "contactor.evening.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431387.0, "controller_id": 335, "user_id": "user"},
        {"value": "1", "tag": "contactor.night.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431382.0, "controller_id": 335, "user_id": "user", "type": "test"},
        {"value": "True", "tag": "connected", "receive_timestamp": 1498463432.3240337,
         "device_timestamp": 1498463432.3240337, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335, "type": "test"},
        {"value": "1", "tag": "contactor.evening.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431387.0, "controller_id": 335, "user_id": "user"},
        {"value": "1", "tag": "contactor.night.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431382.0, "controller_id": 335, "user_id": "user", "type": "test"},
        {"value": "True", "tag": "connected", "receive_timestamp": 1498463432.3240337,
         "device_timestamp": 1498463432.3240337, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335},
        {"value": "0", "tag": "controller.common-mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498463443.841, "controller_id": 335, "type": "test"},
        {"value": "1", "tag": "contactor.evening.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431387.0, "controller_id": 335, "user_id": "user"},
        {"value": "1", "tag": "contactor.night.mode", "receive_timestamp": 1498463446.486685,
         "device_timestamp": 1498431382.0, "controller_id": 335, "user_id": "user", "type": "test"},
    ]
    i = 0
    while i < len(data):
        conn.send(body=json.dumps(data[i]), destination=config["RABBIT_MQ"]["queue"])
        logging.info("Package was send")

        i += 1
        if i == len(data):
            i = 0
