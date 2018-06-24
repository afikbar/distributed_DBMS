from kafka import KafkaConsumer
import re
import json
import argparse, elasticsearch, json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def tal_consumer(ip, port, topic):
    consumer = KafkaConsumer(topic, 
                            bootstrap_servers='{}:{}'.format(ip,port), 
                            auto_offset_reset='earliest')
    first = next(consumer).value
    schema = first.decode('utf8')

    schema = re.sub("[()']",'',schema).split(",")
    schema = [word.strip() for word in schema]

    # ls_dict = []
    es = Elasticsearch()  # use default of localhost, port 9200
    i=1
    for msg in consumer:
        val = msg.value.decode('utf8')
        val_ls = re.sub("[()']",'',val).split(",")
        val_ls = [word.strip() for word in val_ls]
        # ls_dict.append(dict(zip(schema,val_ls)))
        my_json = json.dumps(dict(zip(schema,val_ls)))
        # go to elastic.......
        es.index(index='fpa', doc_type='json', id=i, body=my_json)
        i+=1


# In[26]:


tal_consumer('104.209.178.73','5601','FPA')