import csv
import json
import sys
from pathlib import Path
from typing import Dict, List

from config import (CONFIG_PATH, DATA_DIR, KAFKA_TOPIC_PREFIX, PU_LOCATIONID,
                    read_ccloud_config)
from confluent_kafka import KafkaException, Producer


class PuLocationIDProducer(Producer):
    def __init__(self, props: Dict):
        super().__init__(props)

    @staticmethod
    def read_records(resource_path: str):
        records = []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            header = list(map(lambda x: x.lower(), header))
            pu_locationid_index = header.index(PU_LOCATIONID.lower())

            MAX_ROWS = 300

            for idx, row in enumerate(reader):
                if not row[pu_locationid_index]:
                    MAX_ROWS += 1
                    continue
                records.append(row[pu_locationid_index] or None)

                if idx == MAX_ROWS:
                    break
        return records

    def publish_rides(self, topic: str, messages: List[int]):
        for pu_locationid in messages:
            key = json.dumps({PU_LOCATIONID: pu_locationid}).encode('utf-8')
            value = json.dumps({PU_LOCATIONID: pu_locationid}).encode('utf-8')
            try:
                self.poll(0)
                self.produce(topic=topic, key=key, value=value)
                print(f'Record {pu_locationid} successfully produced')
            except (BufferError, KafkaException, NotImplementedError) as e:
                if isinstance(e, BufferError):
                    print('Buffer error, the queue must be full! Polling with timeout...')
                    self.poll(0.1)

                    print('Resuming...')
                    self.produce(topic=topic, key=key, value=value)
                else:
                    print(e.__str__())
                    break


if __name__ == '__main__':
    file_name = sys.argv[1]
    data_path = DATA_DIR + file_name
    data_name = file_name.split('_')[0]
    topic_name = KAFKA_TOPIC_PREFIX + '_' + data_name

    path = Path(__file__).parent
    configs = (path / CONFIG_PATH).resolve()
    props = read_ccloud_config(configs)
    props['linger.ms'] = 5

    producer = PuLocationIDProducer(props)
    rides_pu_locationid = producer.read_records(resource_path=data_path)

    producer.publish_rides(topic=topic_name, messages=rides_pu_locationid)
    producer.flush()
