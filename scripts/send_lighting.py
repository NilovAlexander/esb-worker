import time
import json
import random
import argparse
from datetime import datetime

import stomp


CONFIG_DESTINATION = 'lighting'
STATUSES = ('online', 'offline')

parser = argparse.ArgumentParser()
parser.add_argument(
    '--district_id',
    help='Id района',
    default=1,
    type=int
)
parser.add_argument(
    '--messages_num',
    help='Кол-во сообщений',
    default=10,
    type=int
)
parser.add_argument(
    '--timeout',
    help='Задержка перед отправкой, сек.',
    default=10,
    type=int
)
parser.add_argument(
    '--host',
    help='Хост брокера',
    default='localhost',
    type=str
)
parser.add_argument(
    '--port',
    help='Порт брокера',
    default=61613,
    type=int
)
parser.add_argument(
    '--destination',
    help='Название очереди',
    default='opcua_shuno_lighting',
    type=str
)

args = parser.parse_args()

hosts = [(args.host, args.port)]
conn = stomp.Connection(host_and_ports=hosts)
conn.start()
conn.connect(wait=True)

for i in range(args.messages_num):
    data = {
        'district_id': args.district_id,
        'controller_id': args.district_id,
        'lighting_id': args.district_id,
        'status': random.choice(STATUSES),
        'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    print(data)
    conn.send(
        body=json.dumps(data),
        destination=args.destination
    )
    time.sleep(args.timeout)

conn.disconnect()
