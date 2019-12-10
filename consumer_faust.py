import faust
import pymongo
from geopy.distance import geodesic
from datetime import datetime
import pytz
import json
app = faust.App('consumer',broker='kafka://10.168.0.2:9092',value_serializer='raw', consumer_auto_offset_reset="latest")
from bson import ObjectId

topic_log = app.topic("alog")

tz = pytz.timezone('Asia/Shanghai')
mongo_topic = app.topic('mongo')
elastic_topic =app.topic('elastic')
def convert_document(data):
    document = {}
    document['taxi_id']=int(data[0])
    document['timestamp'] = data[1]
    document['geo_point']={'lat':float(data[3]),'lon':float(data[2])}
    document["mph"]=data[4]
    document["distance"]=data[5]
    document["halt_time"]=data[6]
    return document

def find_stat(taxi):
    log_topic.send("Will find the stats now")
    last_entry = mycol.find_one({"taxi_id": int(taxi[0])})
    
    #If this the first entry of the taxi
    if last_entry is None:
        return [0,0,0]
    log_topic.send("Taxi%s found in tht databse" % str(taxi[0]))
    last_cursor = mycol.find({"taxi_id": int(taxi[0])},{"_id":0,"timestamp":1,"geo_point.lat":1,\
            "geo_point.lon":1,"distance":1,"halt_time":1,"mph":1}).limit(1).sort([("_id",-1)])
    last=dict()
    for cursor in last_cursor:
        last = dict(cursor)
    origin = (last['geo_point']['lat'],last['geo_point']['lon'])
    dest = (float(taxi[3]),float(taxi[2]))
    
    distance = last['distance']
    time_difference = taxi[1]-last['timestamp']
    halt_time = last['halt_time']
    print("origin is",origin,taxi[0])
    print("destination is ",dest,taxi[0])
    if origin != dest:
        added_distance = calculate_distance(origin,dest)
        time_hr = time_difference/3600
        if time_difference ==0:
            speed = last["mph"]
        else:
            speed = added_distance/time_hr
        if speed >=150:
            speed=last["mph"]
        distance+=added_distance
    else:
        speed = 0
        halt_time+=time_difference

    topic_logs.send("updating the following values %s , %s and %s", str(speed),str(distance),str(halt_time))
    return [speed,distance,halt_time]

def convert_time_epoch(regular_date):
    regular_date = tz.localize(datetime.strptime(regular_date,'%Y-%m-%d %H:%M:%S'), is_dst=None)
    return int((regular_date - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds())
    
def calculate_distance(origin,dest):
    return (geodesic(origin,dest)).miles

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['project']
mycol = mydb["taxi"]
@app.agent(mongo_topic)
async def greet(mongo_topic):
    async for taxi in mongo_topic:
        global mycol
        taxi = taxi.decode('utf-8').split(",")
        await log_topic.send("Consumer - new entry %s", str(taxi))

        taxi[1]=convert_time_epoch(taxi[1])
        taxi.extend(find_stat(taxi))
        doc = convert_document(taxi)
        await log_topic.send("Consumer - push entry %s" % str(doc))
        doc_json = json.dumps(doc)
        await elastic_topic.send(value=doc_json)
        mycol.insert(doc)
        await log_topic.send("Consumer - Entry is sent")
