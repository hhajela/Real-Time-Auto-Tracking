import json
from time import sleep
import datetime
import logging
import traceback
from kafka import KafkaProducer

logger=None
def set_log():
    try:
        
        global logger
        if logger:
            return logger


        logger = logging.getLogger('producer')
        logger.setLevel(logging.INFO)
        # create file handler which logs even debug messages
        fh = logging.FileHandler('producer.log')
        # create console handler with a higher log level
        #ch = logging.StreamHandler()
        #ch.setLevel(logging.ERROR)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        #ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(fh)
        #logger.addHandler(ch)
        return logger
    except Exception:
        print("problem while setting logger")
        traceback.print_exc()




def publish_message(producer_instance, topic_name, key, value):
    try:
        log = set_log()
        log.info("We are publish the message with key %s "%key)
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(','.join(value), encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        log.info("producer added the message successfully ")
    except Exception as e:
        log.error("Error in publishing message")
        log.error(str(e))

def connect_kafka_producer():
    _producer = None
    try:
        log = set_log()
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        log.error('Exception while connecting Kafka')
        log.error(str(ex))
    finally:
        return _producer

def get_taxis():

    try:
        log = set_log()
        #create producer instance
        _producer = connect_kafka_producer()
        topic_name1 = "mongo"
        topic_name2 = "elastic"
        log.info("Successfully created the producer instance")
        data_file = open("sorted_new.csv","r")
        log.info("Successfully opened the data file")
        line = data_file.readline().strip()
        line = line.split(',')
        start_time = datetime.datetime.strptime(line[2],'%Y-%m-%d %H:%M:%S')
        end_time = start_time+datetime.timedelta(minutes=1)
        log.info("Will publish data between %s and %s"% (str(start_time), str(end_time)))
        publish_message(_producer, topic_name1,line[0],line[1:])
        publish_message(_producer, topic_name2,line[0],line[1:])
        for line in data_file:
            line = line.strip().split(',')
            current_time = datetime.datetime.strptime(line[2],'%Y-%m-%d %H:%M:%S')
            while current_time > end_time:
                print(end_time)
                end_time = end_time+datetime.timedelta(minutes=1)
                sleep(60)
            publish_message(_producer,topic_name1, line[0], line[1:])
            publish_message(_producer,topic_name2, line[0], line[1:])


    except Exception as e:
        log.error("Error while getting the taxis")
        log.error(str(e))
if __name__ =='__main__':
    get_taxis()
