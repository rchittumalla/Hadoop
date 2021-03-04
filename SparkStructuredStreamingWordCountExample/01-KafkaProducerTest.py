from kafka import KafkaProducer
from kafka.errors import KafkaError

# configure multiple retries
producer = KafkaProducer(bootstrap_servers=['c.insofe.edu.in:9092'], retries=5)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('Error:', exc_info=excp)

# produce asynchronously with callbacks   
for i in range(100):
    producer.send('insofe_topic_batch43', key=b'key%d' % i, value= b'msg%d' % i).add_callback(on_send_success).add_errback(on_send_error)   
      
# block until all async messages are sent
producer.flush()
