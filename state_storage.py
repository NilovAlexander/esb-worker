import logging
from base64 import urlsafe_b64encode
from datetime import datetime
from functools import partial
from uuid import uuid4

import msgpack
from redis import StrictRedis

from etl_events_worker.constants import (
    ALARM_TAG_LIST, EVENTS_KEY, EVENTS_ZCARD, STATE_KEY
)

dumps = partial(msgpack.dumps, use_bin_type=True)
loads = partial(msgpack.loads, encoding='utf-8', use_list=False)


def make_id():
    """Make uniq short id"""
    return urlsafe_b64encode(uuid4().bytes).rstrip(b'=').decode()


class StateStorage:
    """Hot storage interfaces
    """

    def __init__(self, redis_storage_conn):
        """
        :param redis_storage_conn: redis client
        """
        self.connection = redis_storage_conn
        self.client = StrictRedis(connection_pool=self.connection)
        logging.info('State storage connection')

    def set_state(self, controller_id, state, device_prefix=''):
        """Update state in ot storage
        
        :param int controller_id: controller ID
        :param dict state: state data
        :param str device_prefix: <ahp|lighting|shuno>
        """
        (self.client.pipeline(transaction=False)
         .hmset(STATE_KEY.format(device_prefix, controller_id), dict(
            (k, dumps(v)) for k, v in state.items()
        )
                )
         .execute())

    def add_events(self, controller_id, events):
        """Add events to hot storage

        :param int controller_id: controller ID
        :param list events: event data
        """
        payloads = []
        [payloads.extend((e['ts'], dumps(e))) for e in events]
        (self.client.pipeline(transaction=False)
         .zadd(EVENTS_KEY.format(controller_id), *payloads)
         .zremrangebyrank(EVENTS_KEY.format(controller_id), 0, -EVENTS_ZCARD)
         .execute())

    # method from client.StateStorage
    # TODO remove
    def put(self, event, alive=True, ignore_alive=False):
        """Store events

        Method stores events for provided controller and
        simultaneously and also updates states.

        :param bool ignore_alive: if True update alive in state
        :param timestamp ts: current time as unix timestamp
        :param dict event: dict with required `name` fields,
                            other fields are optional and
                            defined by application logic
        :param bool alive: update controller alive state with provided value
        """
        state = {}
        controller_id = event['controller_id']
        ts = datetime.now().timestamp()

        # event work
        event['ts'] = ts
        event['id'] = make_id()
        if event['tag'] in ALARM_TAG_LIST and event['value']:
            state['acknowledged'] = dumps(
                {'ts': ts, 'value': False, 'name': 'acknowledged'}
            )
        score = ts
        payloads = ((score, dumps(event)),)

        # state work
        state[event['name']] = dumps(event)

        # drop acknowledged if new alarm
        if event['tag'] in ALARM_TAG_LIST and event['value']:
            state['acknowledged'] = dumps(
                {'ts': ts, 'value': False, 'name': 'acknowledged'}
            )

        # logic of acknowledgment if loss of connection
        if not ignore_alive:
            if 'acknowledged' not in state:
                state['alive'] = dumps(
                    {'ts': ts, 'value': alive, 'name': 'alive'}
                )

        (self.client.pipeline(transaction=False)
         .zadd(EVENTS_KEY.format(controller_id), *payloads)
         .hmset(STATE_KEY.format(controller_id), state)
         .execute())

    def disconnect(self):
        self.connection.disconnect()
