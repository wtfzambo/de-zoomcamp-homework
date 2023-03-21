from typing import Dict

CONFIG_PATH = '../client.properties'
DATA_DIR = '../resources/'
KAFKA_TOPIC_PREFIX = 'rides'
PU_LOCATIONID = 'PULocationID'


def read_ccloud_config(config_file) -> Dict[str, str]:
    conf = {}
    with open(config_file, 'r') as fh:
        for line in fh:
            line = line.strip()
            if len(line) and line[0] != '#':
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf
