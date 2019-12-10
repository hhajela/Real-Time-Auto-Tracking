import json
from time import sleep
import datetime
import logging
import traceback
from kafka import KafkaProducer

def send_log(message)
    try:
        topic_log = "alog"
        _producer  = connect_kafka_producer()
        
        publish_message(_producert,topic_log, 1, message)


    except Exception:
        print("problem while setting logger")
        traceback.print_exc()




def publish_message(producer_instance, topic_name, key, value):
    try:
        log = set_log()
        send_log("Producer- Publish Message -  key %s "%key)
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(','.join(value), encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        send_log("Producer Publish successfully ")
    except Exception as e:
        send_log("Error in publishing message for %s",key)
        send_log("Error for failure",str(e))

def connect_kafka_producer():
    _producer = None
    try:
        log = set_log()
        _producer = KafkaProducer(bootstrap_servers=['10.168.0.2:9092'], api_version=(0, 10))
    except Exception as ex:
        send_log('Producer - Exception while Creating Producer object')
        log.error("Above Exception us"str(ex))
    finally:
        return _producer

def get_taxis():

    try:
        log = set_log()
        #create producer instance
        _producer = connect_kafka_producer()
        topic_name1 = "mongo"
        send_log("Producer - Successfully created the producer instance")
        data_file = open("sorted_new.csv","r")
        send_log("Producer -Successfully opened the data file")
        line = data_file.readline().strip()
        line = line.split(',')
        start_time = datetime.datetime.strptime(line[2],'%Y-%m-%d %H:%M:%S')
        end_time = start_time+datetime.timedelta(minutes=1)
        send_log("Producer - Will publish data between %s and %s"% (str(start_time), str(end_time)))
        publish_message(_producer, topic_name1,line[0],line[1:])
        for line in data_file:
            line = line.strip().split(',')
            current_time = datetime.datetime.strptime(line[2],'%Y-%m-%d %H:%M:%S')
            while current_time > end_time:
                print(end_time)
                end_time = end_time+datetime.timedelta(minutes=1)
                sleep(60)
            publish_message(_producer,topic_name1, line[0], line[1:])


    except Exception as e:
        log.error("Error while getting the taxis")
        log.error(str(e))
if __name__ =='__main__':
    get_taxis()
