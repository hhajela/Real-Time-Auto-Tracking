from kafka import KafkaConsumer


from time import sleep
import logging
import traceback
from kafka import KafkaProducer
import sys

logger=None
def set_log():
    try:
        global logger
        if logger:
            return logger


        logger = logging.getLogger('consumer')
        # create file handler which logs even debug messages
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler('consumer.log')
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

def run_consumer(topic_name):
    try:
        log = set_log()
        consumer = KafkaConsumer(topic_name, auto_offset_reset='latest', \
                bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
        log.info("Consumer is set successfully")
        
        for msg in consumer:
            print(msg.value)
        
        while True:
            if not consumer:
                sleep(20)
            for msg in consumer:
                log.info("printing the mesg with key %s " %msg.key)
                print(msg.value)
        
    except Exception as e:
        log.error("problem while consumer")
        log.error(str(e))
    finally:
        print("hello")
        consumer.close()
    
    
if __name__ == '__main__':
    topic_name = sys.argv[1]
    run_consumer(topic_name)
