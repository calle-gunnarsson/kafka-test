#!/usr/bin/env python
# -*- coding: utf-8 -*-
from confluent_kafka import Consumer, KafkaError
import io
import avro
import avro.io
import avro.schema

schema = avro.schema.Parse(open("/code/schema.avsc", "rb").read())

c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'consumers',
    'client.id': 'pysumer',
    'default.topic.config': {
        'offset.store.method': 'broker',
        'auto.offset.reset': 'earliest'
    }
})

c.subscribe(['foobar'])

print("Created Consumer pysumer")

print(c.assignment())

running = True
while running:
    msg = c.poll()
    if not msg.error():
        bytes_reader = io.BytesIO(msg.value())
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        ad = reader.read(decoder)
        print('{}[{}]@{:<4} - {:<6} - id: {:<4} subject: {:<7} price: {:<6}'.format(msg.topic(), msg.partition(), msg.offset(), msg.key().decode('utf-8'), ad['id'], ad['subject'], ad['price']))
    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False

c.close()
