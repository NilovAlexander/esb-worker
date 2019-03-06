import configparser
import unittest

import stomp
import time
from psycopg2 import connect
import json
from datetime import timezone

from etl_events_worker.listeners import EtlEventWorker


class EtlEventTest(unittest.TestCase):
    """
    Тестирование etl демона по заливке данных в pgsql.
    """

    def setUp(self):
        self.maxDiff = None
        config = configparser.RawConfigParser()
        config.read("../../conf/etl_events_test.conf")

        self._db_conn = connect(**config["DB"])
        self.cr = self._db_conn.cursor()
        self.cr.execute("""CREATE TABLE IF NOT EXISTS device_event
            (
              id SERIAL PRIMARY KEY NOT NULL,
              value VARCHAR(255) NOT NULL,
              tag VARCHAR(255) NOT NULL,
              receive_time TIMESTAMP WITHOUT TIME ZONE,
              device_time TIMESTAMP WITHOUT TIME ZONE,
              controller_id INTEGER NULL,
              user_id VARCHAR(100) NULL,
              type VARCHAR(100) NULL
            );""")
        self._db_conn.commit()

        self.mq_conn = stomp.Connection([(config["RABBIT_MQ"]["host"], int(config["RABBIT_MQ"]["port"]))])
        self.mq_conn.start()
        self.mq_conn.connect(config["RABBIT_MQ"]["user"], config["RABBIT_MQ"]["password"], wait=True)

        self._queue_name = config["RABBIT_MQ"]["queue"]
        self._test_worker = EtlEventWorker(config["RABBIT_MQ"], config["DB"])

    def test_require_event(self):
        test_event = {
            "value": "True",
            "tag": "controller.common-mode",
            "receive_timestamp": 1498463432.3240,
            "device_timestamp": 1498463432.3240,
        }
        self._send_to_mq(test_event)

        row = self._get_db_row()
        self.assertIsNotNone(row)

        result = {
            "value": row[0],
            "tag": row[1],
            "receive_timestamp": format_result_time(row[2], 4),
            "device_timestamp": format_result_time(row[3], 4),
        }

        self.assertDictEqual(test_event, result)

    def test_event_with_type(self):
        test_event = {
            "value": "0",
            "tag": "controller.common-mode",
            "receive_timestamp": 1498463446.486,
            "device_timestamp": 1498463443.841,
            "type": "test"
        }
        self._send_to_mq(test_event)

        row = self._get_db_row()
        self.assertIsNotNone(row)

        result = {
            "value": row[0],
            "tag": row[1],
            "receive_timestamp": format_result_time(row[2], 3),
            "device_timestamp": format_result_time(row[3], 3),
            "type": row[6]
        }

        self.assertDictEqual(test_event, result)

    def test_event_with_user(self):
        test_event = {
            "value": "1",
            "tag": "contactor.evening.mode",
            "receive_timestamp": 1498463446.486,
            "device_timestamp": 1498463443.0,
            "user_id": "user"
        }
        self._send_to_mq(test_event)

        row = self._get_db_row()
        self.assertIsNotNone(row)

        result = {
            "value": row[0],
            "tag": row[1],
            "receive_timestamp": format_result_time(row[2], 3),
            "device_timestamp": format_result_time(row[3], 1),
            "user_id": row[5]
        }

        self.assertDictEqual(test_event, result)

    def test_event_with_controller(self):
        test_event = {
            "value": "false",
            "tag": "connected",
            "receive_timestamp": 1498463446.486,
            "device_timestamp": 1498431387.0,
            "controller_id": 335,
        }
        self._send_to_mq(test_event)

        row = self._get_db_row()
        self.assertIsNotNone(row)

        result = {
            "value": row[0],
            "tag": row[1],
            "receive_timestamp": format_result_time(row[2], 3),
            "device_timestamp": format_result_time(row[3], 1),
            "controller_id": row[4]
        }

        self.assertDictEqual(test_event, result)

    def test_full_event(self):
        test_event = {
            "value": "True",
            "tag": "controller.common-mode",
            "receive_timestamp": 1498463432.324,
            "device_timestamp": 1498463432.324,
            "controller_id": 335,
            "user_id": "user",
            "type": "test"
        }
        self._send_to_mq(test_event)

        row = self._get_db_row()
        self.assertIsNotNone(row)

        result = {
            "value": row[0],
            "tag": row[1],
            "receive_timestamp": format_result_time(row[2], 3),
            "device_timestamp": format_result_time(row[3], 3),
            "controller_id": row[4],
            "user_id": row[5],
            "type": row[6]
        }

        self.assertDictEqual(test_event, result)

    def _send_to_mq(self, json_data):
        test_event = json.dumps(json_data)
        self.mq_conn.send(body=test_event, destination=self._queue_name)
        time.sleep(10)

    def _get_db_row(self):
        self.cr.execute("SELECT value, tag,  receive_time, device_time, controller_id, user_id, type "
                        "FROM device_event LIMIT 1;")

        return self.cr.fetchone()

    def tearDown(self):
        self._test_worker.stop()
        self.cr.execute("DROP TABLE device_event;")
        self._db_conn.commit()


def format_result_time(dtm, digits):
    return round(dtm.replace(tzinfo=timezone.utc).timestamp(), digits)
