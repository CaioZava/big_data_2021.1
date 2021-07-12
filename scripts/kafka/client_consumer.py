from kafka import KafkaConsumer
from json import loads
from time import sleep
import ast
consumer = KafkaConsumer(
    'client_side',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id2',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)
for event in consumer:
    event_data = event.value
    # Do whatever you want
    #dict_str = event_data.decode("UTF-8")
    mydata = ast.literal_eval(event_data['line'])
    print(mydata)
    sleep(0.15)