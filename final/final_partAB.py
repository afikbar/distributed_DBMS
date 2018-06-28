
# coding: utf-8

# In[23]:


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


# In[15]:


import argparse, elasticsearch, json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200
es.index(index='posts', doc_type='blog', id=4, body='{"id": "CA-2013-3970925", "state": "CA", "stop_date": "2013-11-20", "stop_time": "", "location_raw": "Amador", "county_name": "Amador County", "county_fips": "06005", "fine_grained_location": "", "police_department": "", "driver_gender": "M", "driver_age_raw": "25-32", "driver_age": "", "driver_race_raw": "White", "driver_race": "White", "violation_raw": "Motorist/Public Service", "violation": "Other", "search_conducted": "FALSE", "search_type_raw": "No Search", "search_type": "", "contraband_found": "FALSE", "stop_outcome": "Motorist/Public Service", "is_arrested": "FALSE", "ethnicity": "W"}')


# In[25]:


es.get(index="fpa", doc_type="json", id=4)['_source']

