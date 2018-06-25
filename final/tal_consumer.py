from kafka import KafkaConsumer
import re
import json
import argparse, elasticsearch, json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
from enum import Enum

class City(Enum):
    San_Diego = 1
    Redding = 2
    Buttonwillow = 3
    Modesto = 4
    
SCHEMA_DEF = ['id', 'stop_date', 'location_raw', 'driver_gender', 
                'driver_race', 'violation', 'search_conducted', 'is_arrested']   

def msg_cleaner(msg):
    result = re.sub("[()']",'',msg.value.decode('utf8')).split(",")
    return [word.strip() for word in result]

def str_to_bool(str):
    return str.lower() == "true"

def data_filter(msg_dict):#returns true if data is ok
    
    return msg_dict['location_raw'].title() in ['San Diego', 'Redding', 'Buttonwillow', 'Modesto'] and\
            msg_dict['search_conducted'].lower() in ['true','false'] and\
            msg_dict['is_arrested'].lower() in ['true','false'] and\
            msg_dict['driver_gender'].lower() in ['m','f'] and\
            re.match('(\d{4})[/.-](\d{2})[/.-](\d{2})$', msg_dict['stop_date'])
                        
def data_convert(msg_dict):
    msg_dict['search_conducted'] = str_to_bool(msg_dict['search_conducted'])
    msg_dict['is_arrested'] = str_to_bool(msg_dict['is_arrested'])
    msg_dict['stop_date'] = datetime.strptime(msg_dict['stop_date'], "%Y-%m-%d")
    msg_dict['driver_gender'] = msg_dict['driver_gender'].upper()
    msg_dict['location_raw'] = msg_dict['location_raw'].title()
    
def tal_consumer(ip, port, topic):
    consumer = KafkaConsumer(topic, 
                            bootstrap_servers='{}:{}'.format(ip,port), 
                            auto_offset_reset='earliest')
    first = next(consumer)
   
    schema = msg_cleaner(first)
    
    es = Elasticsearch() 
    i=1
    for msg in consumer:
        print("message number:",i)
        val_ls = msg_cleaner(msg)
        #keeps only wanted data
        msg_dict = dict([(k,v) for k,v in zip(schema,val_ls) if k in SCHEMA_DEF])
#         temp = msg_dict['id']
        if not data_filter(msg_dict): 
            continue
        
        data_convert(msg_dict)
              
        # go to elastic.......
        # spiderpig - buttonwillow
        loc_json = json.dumps({"location_raw":msg_dict["location_raw"]})
        es.index(index='buttonwillow', doc_type='json', id=i, body=loc_json)
        print("sent to buttonwillow")
        # black panther - redding:
        if msg_dict['search_conducted'] and\
            msg_dict['stop_date'] >= datetime(2016,1,30):
            bp_json = json.dumps(msg_dict)
            es.index(index='reddings', doc_type='json', id=i, body=bp_json)
            print("sent to reddings")
        else:
            #spiderman all genders
            gender_json = json.dumps({"driver_gender":msg_dict["driver_gender"]})
            es.index(index='modesto_gender', doc_type='json', id=i, body=gender_json)
            print("sent to modesto_gender")
            # superman - San Diego
            if msg_dict['driver_gender'] == 'F': #all females from bp_left
                viol_json = json.dumps({"violation":msg_dict["violation"]})
                es.index(index='sandiego_viol', doc_type='json', id=i, body=viol_json)
                print("sent to sandiego_viol")
                if msg_dict['location_raw'] != 'Modesto': #
                    arstd_json = json.dumps({"is_arrested":msg_dict["is_arrested"]})
                    es.index(index='sandiego_arrested', doc_type='json', id=i, body=arstd_json)
                    print("sent to sandiego_arrested")
                    
                
            
            # spiderman - Modesto
            else:
                arstd_json = json.dumps({"is_arrested":msg_dict["is_arrested"]})
                es.index(index='sandiego_arrested', doc_type='json', id=i, body=arstd_json)
                print("sent to sandiego_arrested")
            
            
            
        
        i+=1


# In[26]:


tal_consumer('104.209.178.73','5601','fpa')