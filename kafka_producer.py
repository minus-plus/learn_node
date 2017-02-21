from googlefinance import getQuotes

from kafka import KafkaProducer

import argparse
improt loggine
import time
import json
improt schedule
import atexit

topic_name = ""
kafka_broker = ""
logger_format = "%(asctime)-15s %(message)s"
logging.basicConfig(format=logger_format)
logger = logging.getLogger("kafka_producer")
logger.setLevel(logging.DEBUG)

def ingest_data(stock, kafka_producer)
	

def disconnect(kafka_producer):
	logger.info("disconnect kafka_producer")
	kafka_producer.close()
	
if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('stock',help ='the stock symbol')
	parser.add_argument('topic_name',help='the kafka topic to push to')
	parser.add_argument('kafka_broker',help='location of kafka broker')
	
	args = parser.parse_args()
	stock = args.stock
	topic_name = args.topic_name
	kafka_broker =args.kafka_broker
	
	schedule.ever(1).second.do()
	atexit.register(disconnect, kafka_producer)
