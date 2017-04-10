from kafka import KafkaConsumer
import io
import avro
import avro.io
import avro.schema

schema = avro.schema.Parse(open("/code/schema.avsc", "rb").read())

consumer = KafkaConsumer(group_id='consumers', client_id="pysumer", bootstrap_servers=['kafka:9092'])
consumer.subscribe("foobar")

#consumer.assign([TopicPartition('foobar', 0)])

print("Created Consumer pysumer")

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    ad = reader.read(decoder)
    print('{}[{}]@{} - {} - id: {}, subject: {}, price: {}'.format(msg.topic, msg.partition, msg.offset, msg.key.decode("utf-8"), ad['id'], ad['subject'], ad['price']))

consumer.close()
