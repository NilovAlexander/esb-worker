import json
import logging
import traceback
from datetime import datetime

import stomp
from psycopg2 import extensions

from state_storage import StateStorage
from constants import DEFAULT_DISTRICT_ID, EXCHANGE_ROUTING_KEY

INSERT_CONTROLLER_Q = '''
INSERT INTO device_event VALUES (
    DEFAULT,
    %(value)s,
    %(tag)s,
    %(receive_timestamp)s,
    %(device_timestamp)s,
    %(controller_id)s,
    %(user_id)s,
    %(type)s
);
'''

INSERT_LIGHTING_Q = '''
INSERT INTO lighting_event VALUES (
    DEFAULT,
    %(value)s,
    %(tag)s,
    %(receive_time)s,
    %(device_time)s,
    %(type)s,
    %(lighting_id)s
);
'''


class BaseEtlListener(stomp.ConnectionListener):
    def __init__(self, db_conn, redis_storage_conn, rmq_ws_publisher):
        self._db_conn = db_conn
        self._db_conn.set_isolation_level(
            extensions.ISOLATION_LEVEL_READ_UNCOMMITTED
        )
        self.db_cr = self._db_conn.cursor()
        self.redis_storage = StateStorage(redis_storage_conn)
        self.rmq_ws_publisher = rmq_ws_publisher

    def on_error(self, headers, message):
        logging.info('received an error  {}'.format(message))

    def _make_message(self, message):
        return json.loads(message)

    def _handle_message(self, headers, message):
        pass

    def on_disconnected(self):
        self.rmq_ws_publisher.close()

    def on_message(self, headers, message):
        logging.info('received a message: {}'.format(message))
        self._handle_message(headers, self._make_message(message))


class EventEtlListener(BaseEtlListener):

    def _handle_message(self, headers, message):
        if message.get("controller_id"):
            try:
                self.redis_storage.add_events(
                    message["controller_id"], [message]
                )
            except:
                logging.error(
                    "Can't load data {message} to hot storage:\n"
                    "{traceback}".format(
                        message=message,
                        traceback=traceback.format_exc()
                    )
                )
        try:
            message.update({
                "device_timestamp": datetime.utcfromtimestamp(
                    message["device_timestamp"]
                ),
                "receive_timestamp": datetime.utcfromtimestamp(
                    message["receive_timestamp"]
                )
            })
            self.db_cr.execute(INSERT_CONTROLLER_Q, message)
            self._db_conn.commit()
        except:
            self._db_conn.rollback()
            logging.error(
                "Can't load data {message} to DB:\n{traceback}".format(
                    message=message,
                    traceback=traceback.format_exc())
            )


class StateEtlListener(BaseEtlListener):
    def _handle_message(self, headers, message):
        try:
            for controller_id, state in message.items():
                self.redis_storage.set_state(
                    controller_id, state
                )
        except:
            logging.error(
                "Can't load data {message} to hot storage:\n"
                "{traceback}".format(
                    message=message,
                    traceback=traceback.format_exc()
                )
            )


class LightingEtlListener(BaseEtlListener):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.declared_district_exchanges = set()

    def _handle_message(self, headers, message):

        try:
            lighting_id = message["lighting_id"]
            tag = message["tag"]
            self.redis_storage.set_state(
                lighting_id,
                {tag: message},
                '_lighting'
            )
        except:
            logging.error(
                "Can't load data {message} to hot storage:\n"
                "{traceback}".format(
                    message=message,
                    traceback=traceback.format_exc()
                )
            )
        try:
            exchange_name = self._declare_exchange(message)
            self.rmq_ws_publisher.publish(message, exchange_name)
        except:
            logging.exception(
                "Can't load data {message} to websocket exchange".format(
                    message=message
                )
            )

        try:
            self.db_cr.execute(
                INSERT_LIGHTING_Q,
                {
                    'value': message['value'],
                    'tag': message['tag'],
                    'receive_time': datetime.utcfromtimestamp(
                        message["device_timestamp"]
                    ),
                    'device_time': datetime.utcfromtimestamp(
                        message["receive_timestamp"]
                    ),
                    'type': message['type'],
                    'lighting_id': message['lighting_id'],
                }
            )
            self._db_conn.commit()
        except:
            self._db_conn.rollback()
            logging.error(
                "Can't load data {message} to DB:\n{traceback}".format(
                    message=message,
                    traceback=traceback.format_exc())
            )

    def _declare_exchange(self, message):
        district_id = message.get('district_id', DEFAULT_DISTRICT_ID)
        exchange_name = self._get_exchange_name(district_id)
        if district_id not in self.declared_district_exchanges:
            self.rmq_ws_publisher.declare_exchange(
                exchange_name=exchange_name
            )
            self.declared_district_exchanges.add(district_id)
            logging.info('Declared exchange: {}'.format(exchange_name))
        return exchange_name

    def _get_exchange_name(self, district_id):
        return 'district_id:{}'.format(district_id)


# TODO move to config or settings
LISTENER_MAP = {
    'event': EventEtlListener,
    'state': StateEtlListener,
    'lighting': LightingEtlListener
}

WORKER_TYPE_LIST = LISTENER_MAP.keys()
