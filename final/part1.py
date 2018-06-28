from kafka import KafkaConsumer
import re
import json

def tal_consumer(ip, port, topic):
    consumer = KafkaConsumer(topic, 
                            bootstrap_servers='{}:{}'.format(ip,port), 
                            auto_offset_reset='earliest')
    first = next(consumer).value
    schema = first.decode('utf8')

    schema = re.sub("[()']",'',schema).split(",")
    schema = [word.strip() for word in schema]

    # ls_dict = []
    for msg in consumer:
        val = msg.value.decode('utf8')
        val_ls = re.sub("[()']",'',val).split(",")
        val_ls = [word.strip() for word in val_ls]
        # ls_dict.append(dict(zip(schema,val_ls)))
        my_json = json.dumps(dict(zip(schema,val_ls)))
        # go to elastic.......