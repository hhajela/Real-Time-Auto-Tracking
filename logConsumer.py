from kafka import KafkaConsumer


def run_consumer(topic_name):
    try:
        consumer = KafkaConsumer(topic_name, auto_offset_reset='latest',\
                bootstrap_servers=['10.168.0.2:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
        
        while(True):
            for msg in consumer:
                with open('RTAL.log','a') as logf:
                    logf.write('{0}\n'.format(msg.value.decode('utf-8')))
        """
        while True:
            if not consumer:
                sleep(20)
            for msg in consumer:
                logf.write(msg.value.decode('utf-8'))
        """
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print("Exception {0} occurred".format(e))
    finally:
        consumer.close()

if __name__=="__main__":
    run_consumer('alog')
