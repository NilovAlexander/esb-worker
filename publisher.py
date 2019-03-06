import logging
import pika
import json


class ExchangePublisher(object):
    """Exchange Publisher for web-socket
    """
    ROUTING_KEY = 'key'
    EXCHANGE_TYPE = 'fanout'

    def __init__(self, host, user, password):
        self._host = host
        self._user = user
        self._password = password
        self._params = pika.connection.ConnectionParameters(
            host=self._host,
            credentials=pika.credentials.PlainCredentials(
                self._user, self._password
            )
        )
        self._connection = None
        self._channel = None

    def declare_exchange(self, exchange_name):
        if not self._channel:
            self.connect(exchange_name)
        else:
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=self.EXCHANGE_TYPE
            )
        logging.debug('Declared exchange: {}'.format(exchange_name))
        return exchange_name

    def connect(self, exchange_name):
        if not self.connection or self.connection.is_closed:
            self.connection = pika.BlockingConnection(self._params)
            self.channel = self._conn.channel()
            self.declare_exchange(exchange_name=exchange_name)

    def _publish(self, msg, exchange):
        self._channel.basic_publish(exchange=exchange,
                                    routing_key=self.ROUTING_KEY,
                                    body=json.dumps(msg).encode())
        logging.debug('message sent: %s', msg)

    def publish(self, msg, exchange):
        """Publish msg, reconnecting if necessary."""

        try:
            self._publish(msg, exchange)
        except pika.exceptions.ConnectionClosed:
            logging.info('Reconnecting to queue')
            self.connect(exchange)
            self._publish(msg, exchange)

    def close(self):
        self._conn.close()

    @property
    def channel(self):
        return self._channel

    @channel.setter
    def channel(self, channel):
        self._channel = channel

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, connection):
        self._connection = connection
