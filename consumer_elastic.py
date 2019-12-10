import faust
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
import json
import io
from pprint import pprint

es = Elasticsearch()

""" Reads from Kafka, publishes to elasticsearch """

app = faust.App('consumer',broker='kafka://10.168.0.2:9092',value_serializer='raw')

message_topic = app.topic('elastic')
producer = None

def sendLog(message,topic='alog'):
    if producer is not None:
        producer.send(topic,key = bytes('1', encoding='utf-8'),value=bytes(message,encoding='utf-8'))
    else:
        print("Producer not initialized")


@app.agent(message_topic)
async def publishToElastic(stream):
    global es

    if not create_index(es, 'geolocations'):
        sendLog("error occurred while creating index")
    
    async for msgs in stream.take(100,within=10):
        for msg in msgs:
            jsonData = json.loads(msg.decode('utf-8'))
            jsonData['timestamp'] = jsonData['timestamp']*1000
            #add to ES
            insertInES(es, 'geolocations', jsonData)
        #print([mg.decode('utf-8') for mg in msg])

"""
@app.agent()
async def elasticSearchSink(messages):
   # batch writes to elastic search
   global es
   async for msg in messages.take(100,within=10):
      print(msg)
"""

def insertInES(es_object, index_name, record):
    try:
        outcome = es_object.index(index=index_name, body=record)
    except Exception as ex:
        sendLog('Error {0} in indexing data'.format(ex))

def create_index(es_object, index_name='geolocations'):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
                "properties": {
                    "taxi_id": {
                        "type": "long"
                    },
                    "timestamp": {
                        "type": "date"
                    },
                    "geo_point": {
                        "type": "geo_point"
                    },
                    "mph": {
                        "type": "float"
                    },
                    "distance": {
                        "type": "float"
                    },
                    "halt_time": {
                        "type": "long"
                    }
                }
            }
    }
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, body=settings)
            sendLog('Created Index')
        created = True
    except Exception as ex:
        sendLog("Error {0}".format(ex))
    finally:
        return created

if __name__=="__main__":
    producer = KafkaProducer(bootstrap_servers=['10.168.0.2:9092'], api_version=(0, 10))

    app.main()

"""
{'taxi_id': 1, 'timestamp': 1201938068000, 'geo_point': {'lat': 39.91248, 'lon': 116.47186}, 'mph': 0, 'distance': 3.1533672334955427, 'halt_time': 180}
"""

    

    

