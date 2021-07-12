from kafka import KafkaConsumer
from json import loads
from time import sleep
import ast
#import os
import subprocess
import sys


def twint_search(keywords):
    with open('term_history.txt', 'r') as terms:
        terms_lines = terms.read()
    list_of_lines = terms_lines.split("\n")
    if not keywords in terms_lines:
        p = subprocess.Popen([sys.executable, '/mnt/c/Users/Caio/Documents/SNA/Scripts/bckgrounsd_twint.py', '--keyword', keywords])#,
        #                     stdout=subprocess.PIPE,
        #                     stderr=subprocess.STDOUT)
        #os.system("nohup /home/caiozava/anaconda/envs/twint_SNA/bin/python /mnt/c/Users/Caio/Documents/SNA/Scripts/bckgrounsd_twint.py --keyword '" + keywords + "' &")
        list_of_lines.append(keywords)
        with open('term_history.txt', 'w') as terms:
            terms.write("\n". join(list_of_lines))


consumer = KafkaConsumer(
    'twint_side',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)
for event in consumer:
    event_data = event.value
    # Do whatever you want
    #print(event_data)
    #dict_str = event_data.decode("UTF-8")
    mydata = ast.literal_eval(event_data['content'])
    twint_search(mydata['search_str'])
    #print(mydata)
    sleep(0.1)

