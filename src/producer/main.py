#!/usr/bin/env python
# -*- coding: utf-8 -*-import sys

import logging
from time import sleep
from random import uniform, randint
from confluent_kafka import Producer

import avro
import avro.schema
import avro.io
import io

log = logging.getLogger()

schema = avro.schema.Parse(open("/code/schema.avsc", "rb").read())

conf = {'bootstrap.servers': "kafka:9092"}

p = Producer(**conf)

def delivery_callback (err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        print('topic="{}", partition={}, offset={}, timestamp={}'.format(
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.timestamp()[1]
        ))


for i in range(10000):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)

    ad_id = randint(0, i)

    key=bytes('ad{}'.format(ad_id), 'utf-8')
    ad = {"id": ad_id, "subject": 'Ad {}'.format(ad_id), "price": randint(1000,100000)}

    writer.write(ad, encoder)
    raw_bytes = bytes_writer.getvalue()
    p.produce("foobar", value=raw_bytes, key=key, callback=delivery_callback)
    p.poll(0)

    sleep(uniform(0.0001, 0.2))

p.flush()
