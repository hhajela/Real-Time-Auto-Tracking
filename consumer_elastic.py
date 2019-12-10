import faust
from elasticsearch import Elasticsearch
import json
import io

es = Elasticsearch()

""" Reads from Kafka, publishes to elasticsearch """

app = faust.App('consumer',broker='kafka://localhost:9092',value_serializer='raw')

message_topic = app.topic('geoMsg')

@app.agent(message_topic)
async def publishToElastic(stream):
    global es
    if not create_index(es, 'geolocations'):
        print("error occurred while creating index")
    
    async for msg in stream:
        await elasticSearchSink.send(value=json.loads(io.BytesIO(msg).read().decode('utf-8')))

@app.agent()
async def elasticSearchSink(messages):
   # batch writes to elastic search
   global es
   async for msg in messages.take(100):
      print(msg)

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
                    "location": {
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
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


"""
{"taxi_id": 1, "timestamp": 1201938068, 
"geo_point": {"latitude": 39.91248, "longitude": 116.47186}, 
"mph": 0, "distance": 3.1533672334955427, "halt_time": 180, "_id": "5def05732fd95084b1c8017d"}
"""

    

    

