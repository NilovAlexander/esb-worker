# exchanges
DEFAULT_DISTRICT_ID = 1
EXCHANGE_ROUTING_KEY = ''

# hot storage
ALARM_TAG_LIST = [
    'controller.cabinet-door',
    'controller.fire',
    'controller.station-door',
]

ALL = '__all__'
EVENTS_KEY = 'ctrl:{}:events'
EVENTS_ZCARD = 500
STATE_KEY = 'ctrl{}:{}:state'

# listeners
RECONNECT_TIMEOUT = 10
