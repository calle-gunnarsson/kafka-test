import logging
from time import sleep
from random import uniform, randint
from kafka import KafkaProducer
from kafka.errors import KafkaError

import avro
import avro.schema
import avro.io
import io

log = logging.getLogger()

schema = avro.schema.Parse(open("/code/schema.avsc", "rb").read())

producer = KafkaProducer(bootstrap_servers='kafka:9092')

for i in range(10000):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)

    ad_id = randint(0, i)

    key=bytes('ad{}'.format(ad_id), 'utf-8')
    ad = {"id": ad_id, "subject": 'Ad {}'.format(ad_id), "price": randint(1000,100000)}

    writer.write(ad, encoder)
    raw_bytes = bytes_writer.getvalue()
    future = producer.send('foobar', value=raw_bytes, key=key)

    try:
        meta = future.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        log.exception()
        continue

    print('topic="{m.topic}", \
partition={m.partition}, \
topic_partition=({m.topic_partition.topic}, {m.topic_partition.partition}), \
offset={m.offset}, \
timestamp={m.timestamp}, \
checksum={m.checksum}, \
serialized_key_size={m.serialized_key_size}, \
serialized_value_size={m.serialized_value_size}'.format(m=meta))

    #sleep(uniform(0.0001, 0.2))

producer.flush()
producer.close()
