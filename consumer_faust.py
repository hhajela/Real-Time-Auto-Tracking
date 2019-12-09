import faust
import pymongo
from geopy.distance import geodesic
from datetime import datetime
import pytz
app = faust.App('consumer',broker='kafka://localhost:9092',value_serializer='raw')


tz = pytz.timezone('Asia/Shanghai')
mongo_topic = app.topic('mongo')

def convert_document(data):
    document = {}
    document['taxi_id']=int(data[0])
    document['timestamp'] = data[1]
    document['geo_point']={'latitude':float(data[3]),'longitude':float(data[2])}
    document["mph"]=data[4]
    document["distance"]=data[5]
    document["halt_time"]=data[6]
    return document

def find_stat(taxi):
    last_entry = mycol.find_one({"taxi_id": int(taxi[0])})
    #If this the first entry of the taxi
    if last_entry is None:
        return [0,0,0]
    last_cursor = mycol.find({"taxi_id": int(taxi[0])},{"_id":1,"timestamp":1,"geo_point.latitude":1,\
            "geo_point.longitude":1,"distance":1,"halt_time":1}).limit(1).sort([("_id",-1)])
    last=dict()
    for cursor in last_cursor:
        last = dict(cursor)
    print(last)
    origin = (last['geo_point']['latitude'],last['geo_point']['longitude'])
    dest = (float(taxi[3]),float(taxi[2]))
    
    distance = last['distance']
    time_difference = taxi[1]-last['timestamp']
    halt_time = last['halt_time']
    print("origin is",origin,taxi[0])
    print("destination is ",dest,taxi[0])
    if origin != dest:
        added_distance = calculate_distance(origin,dest)
        time_hr = time_difference/3600
        speed = added_distance/time_hr
        distance+=added_distance
    else:
        speed = 0
        halt_time+=time_difference

    
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
        

        taxi[1]=convert_time_epoch(taxi[1])
        taxi.extend(find_stat(taxi))
        doc = convert_document(taxi)
        mycol.insert(doc)
