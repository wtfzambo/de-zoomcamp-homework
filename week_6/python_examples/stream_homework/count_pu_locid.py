import json
from pathlib import Path

import faust
from aiokafka.helpers import create_ssl_context
from config import CONFIG_PATH, PU_LOCATIONID, read_ccloud_config
from faust.types.auth import AuthProtocol, SASLMechanism
from faust.types.streams import StreamT
from ride import Ride, RideCount

path = Path(__file__).parent
config_path = (path / CONFIG_PATH).resolve()
config = read_ccloud_config(config_path)

broker_credentials = faust.SASLCredentials(
    mechanism=SASLMechanism.PLAIN,
    ssl_context=create_ssl_context(),
    username=config['sasl.username'],
    password=config['sasl.password']
)
broker_credentials.protocol = AuthProtocol.SASL_SSL


app = faust.App(
    f'de.zoomcamp.{path.name}',
    broker=f'kafka://{config["bootstrap.servers"]}',
    value_serializer='json',
    broker_credentials=broker_credentials,
    topic_partitions=2,
    topic_replication_factor=3,
)

green_topic = app.topic('rides_green', value_type=Ride, partitions=2)
fhv_topic = app.topic('rides_fhv', value_type=Ride, partitions=2)
rides_all_topic = app.topic('rides_all', key_type=bytes, value_type=RideCount, partitions=2)

pu_locationid_table = app.Table(PU_LOCATIONID.lower(), default=int, partitions=2)


@app.agent(green_topic)
async def process_green(stream: StreamT):
    # WARN: The fucking groupby key must be read as a STR in the fucking
    # event so either the interface must define it as such or one must pass
    # a custom deserializer function of some shit that outputs a fucking string
    async for event in stream.group_by(Ride.PULocationID):  # type: ignore
        assert isinstance(event, Ride)
        pu_locationid_table[event.PULocationID] += 1
        print(f'Count for Pickup Location ID {event.PULocationID}: {pu_locationid_table[event.PULocationID]}')
        key = json.dumps({PU_LOCATIONID: event.PULocationID}).encode('utf-8')
        await rides_all_topic.send(key=key, value=RideCount(count=pu_locationid_table[event.PULocationID]))


@app.agent(fhv_topic)
async def process_fhv(stream: StreamT):
    async for event in stream.group_by(Ride.PULocationID):  # type: ignore
        assert isinstance(event, Ride)
        pu_locationid_table[event.PULocationID] += 1
        print(f'Count for Pickup Location ID {event.PULocationID}: {pu_locationid_table[event.PULocationID]}')
        key = json.dumps({PU_LOCATIONID: event.PULocationID}).encode('utf-8')
        await rides_all_topic.send(key=key, value=RideCount(count=pu_locationid_table[event.PULocationID]))


@app.timer(10.0)
async def log_table_every_10_seconds():
    if app.rebalancing:
        return

    print(PU_LOCATIONID + '\n' + pu_locationid_table.as_ansitable(
        key=PU_LOCATIONID,
        value='count',
        title=f'Total number of rides for each {PU_LOCATIONID}',
        sort=True
    ))


if __name__ == '__main__':
    # Run by executing `faust -A groupby worker -l info` in the CLI

    # Remember to produce some event using the producer class
    # from the json example before running this
    app.main()

    # Delete the changelog topic online if you want to reset the table
