from kafka import KafkaConsumer

class KafkaConsummerWrapper:
    def __init__(self, topic, broker = "localhost:9092", group_id = 'sparkmovie-group'):
        self._consumer = KafkaConsumer(
                            topic, group_id=group_id, bootstrap_servers=[broker]
                         )

    def consume(self, callback):
        for message in self._consumer:
            callback(message)
