import logging
import time

import psycopg2
import stomp
from redis import ConnectionPool

from listeners import LISTENER_MAP
from publisher import Publisher
from constants import RECONNECT_TIMEOUT


class EtlEventWorker(object):
    def __init__(self, rabbit_cfg, rmq_ws_cfg, db_cfg, redis_cfg, worker_type):
        """
        :param dict rabbit_cfg: config rabbit
        :param dict rmq_ws_cfg: config rmq_ws
        :param dict db_cfg: config db
        :param dict redis_cfg: config redis
        :param BaseEtlListener worker_type: Type of worker (listener)
        """
        self.worker_type = worker_type
        self._rmq_user = rabbit_cfg["user"]
        self._rmq_pwd = rabbit_cfg["password"]

        self._queue = rabbit_cfg['{}_queue'.format(self.worker_type)]
        self._ack = 'auto'
        self._db_conn = psycopg2.connect(**db_cfg)

        self.rmq_ws_publisher = Publisher(
            user=rmq_ws_cfg["user"],
            password=rmq_ws_cfg["password"],
            host=rmq_ws_cfg["host"]
        )
        # self.rmq_ws_channel = self.rmq_ws_connection.channel()
        rmq_host = (rabbit_cfg["host"], rabbit_cfg["port"])
        self._conn = stomp.Connection([rmq_host])
        self.redis_storage_conn = ConnectionPool.from_url(
            "redis://{}:{}/{}".format(
                redis_cfg["host"],
                redis_cfg["port"],
                redis_cfg['db']
            )
        )
        listener = LISTENER_MAP[self.worker_type]
        self._conn.set_listener(
            self.worker_type,
            listener(
                db_conn=self._db_conn,
                redis_storage_conn=self.redis_storage_conn,
                rmq_ws_publisher=self.rmq_ws_publisher
            )
        )
        self._subscribe_and_connect()

    def run(self):
        while self._conn.is_connected():
            time.sleep(RECONNECT_TIMEOUT)

        self._subscribe_and_connect()

    def stop(self):
        self._conn.disconnect()
        self.redis_storage_conn.disconnect()

        self.rmq_ws_publisher.close()

    def _subscribe_and_connect(self):
        self._conn.start()
        self._conn.connect(self._rmq_user, self._rmq_pwd, wait=True)
        self._conn.subscribe(
            destination=self._queue,
            id=self.worker_type,
            ack=self._ack
        )


def etl_event_worker_process(rmq_cfg,
                             rmq_ws_cfg,
                             db_cfg,
                             redis_cfg,
                             num,
                             worker_type):
    """Start etl event worker.
    Target for multiprocessing

    :param dict rmq_cfg: config rabbit
    :param dict rmq_ws_cfg: config rmq_ws
    :param dict db_cfg: config db
    :param dict redis_cfg: config redis
    :param BaseEtlListener worker_type: Type of worker (listener)
    """
    logging.info("Worker {} starting {}".format(worker_type, num))
    instance = EtlEventWorker(
        rmq_cfg, rmq_ws_cfg, db_cfg, redis_cfg, worker_type
    )
    instance.run()
