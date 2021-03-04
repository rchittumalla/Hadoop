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

filename = "/home/jayantm/Lab/SparkStrucStreaming/SparkStreamingKafka/SMSSpamCollectionTest"
fileText = open(filename, "r")
		
for line in fileText:
    # Note that the application is responsible for encoding messages to type str
    producer.send("insofe_topic_batch43", line).add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()

