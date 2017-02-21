from kafka improt KafkaConsumer

import argparse
import atexit
import logging
import redis

topic_name = ""
kafka_brokder = ""

# define logger
logger_format = '%(asctime)-15s %(message)s'
loggine.basicConfig(format=logger_format)
logger = logging.getLogger('redis-publisher')
logger.setLevel(loggging.DEBUG)

def disconnect(kafka_consumer):
	logger.info("disconnect kafka_consumer")
	kafka_consumer.close()

def publish_to_redis(redis_client, redis_channel, kafka_consumer):
	for msg in kafka_consumer:
		logger.info("Received new data from kafka: %s" % str(msg))
		redis_client.publish(redis_channel, msg.value)
if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name',help='the kafka topic consume form')
	parser.add_argument('kafka_broker',help='the location of the kafka_broker')
	parser.add_argument('redis_channel',help='the redis redis_channel to publish to')
	parser.add_argument('redis_host',help='the location of the redis server')
	parser.add_argument('redis_port',help='the redis port')
	
	args = paser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	redis_channel = args.redis_channel
	redis_host = args.redis_host
	redis_port = args.redis_port
	
	# get kafka_consumer
	kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)
	
	# get redis_client
	redis_client = redis.Redis(host=redis_host, port=6379, db=0)
	atexit.register(disconnect, kafka_consumer)
	# publish message to redis
	publish_to_redis(redis_client, redis_channel, kafka_consumer)
