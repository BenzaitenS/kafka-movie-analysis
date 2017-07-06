from kafka import KafkaProducer

class KafkaProducerWrapper:
    def __init__(self, broker = "localhost:9092", every_n = 1000):
        self._producer = KafkaProducer(bootstrap_servers=[broker])

    def produce(self, data, topic):
        try:
            self._producer.send(topic, data.encode('utf-8'))
        except Exception as e:
            print(e)

    def flush(self):
        self._producer.flush()
