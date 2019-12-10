from kafka import KafkaConsumer


def run_consumer(topic_name):
    logf = None
    try:
        consumer = KafkaConsumer(topic_name, auto_offset_reset='latest', \
                bootstrap_servers=['10.168.0.2:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
        logf = open('RTAL.log','a')
        
        for msg in consumer:
            logf.write(msg.value)
        
        while True:
            if not consumer:
                sleep(20)
            for msg in consumer:
                log.info("printing the mesg with key %s " %msg.key)
                print(msg.value)
        
    except Exception as e:
        logf.write("Exception {0} occurred".format(e))
    finally:
        consumer.close()
        if logf is not None:
            logf.close()

if __name__=="__main__":
    run_consumer('alog')
