#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

import logging
from time import sleep
from random import uniform, randint
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

log = logging.getLogger()

value_schema = avro.load('/code/value_schema.avsc')
key_schema = avro.load('/code/key_schema.avsc')

p = AvroProducer({
        'bootstrap.servers': 'kafka:9092',
        'schema.registry.url': 'http://registry:8081',
    },
    default_value_schema=value_schema,
    default_key_schema=key_schema
)


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
    ad_id = randint(0, i)

    key='ad{}'.format(ad_id)
    ad = {"id": ad_id, "subject": 'Ad {}'.format(ad_id), "price": randint(1000,100000)}

    p.produce(topic="foobar", value=ad, key=key, callback=delivery_callback)
    p.poll(0)

    sleep(uniform(0.0001, 0.2))

p.flush()
