#!/usr/bin/env python
# -*- coding: utf-8 -*-
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

c = AvroConsumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'consumers',
    'client.id': 'pysumer',
    'schema.registry.url': 'http://registry:8081',
    'default.topic.config': {
        'offset.store.method': 'broker',
        'auto.offset.reset': 'earliest'
    }
})

c.subscribe(['foobar'])

print("Created Consumer pysumer")

print(c.assignment())

running = True
msg=None
while running:
    try:
        msg = c.poll(10)
        if msg:
            if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
            ad = msg.value()
            print('{}[{}]@{:<4} - {:<6} - id: {:<4} subject: {:<7} price: {:<6}'.format(msg.topic(), msg.partition(), msg.offset(), msg.key(), ad['id'], ad['subject'], ad['price']))
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False

c.close()
