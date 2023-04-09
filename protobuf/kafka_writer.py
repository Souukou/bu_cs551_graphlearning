import kafka
import message_pb2


producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092',
                               value_serializer=lambda x: x.SerializeToString())

while True:
  event = message_pb2.Event()
  event.source = 1
  event.neighbors = "2-3-4"
  event.label = 2
  event.data_hex = "adrish"
  event.type = message_pb2.NODE
  producer.send("test", value=event)
