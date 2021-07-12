from time import sleep
from json import dumps
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
request = input('type the hashtag to be searched:')
data = str({'search_str':request})
data = {'content': data}
producer.send('twint_side', value=data)


