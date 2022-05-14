import logging

import pika
from pika.exceptions import AMQPConnectionError



class BaseConnector:
	"""
	Base parent for any connector with connection logic to the broker.
	"""
	def __init__(self, host, port):
		self.host = host
		self.port = port

	def connect(self):
		try:
			self.connectionParams = pika.ConnectionParameters(host=self.host, port=self.port, heartbeat=600, blocked_connection_timeout=300)
			self.rabbitConnection = pika.BlockingConnection(self.connectionParams)

		except AMQPConnectionError:
			logging.error(f"Connection to RabbitMQ on {self.host}:{self.port} is refused by server.")
			raise AMQPConnectionError

	def configure(self, exchange, exchangeType, routing_keys=None):
		pass


class RabbitConsumerConnector(BaseConnector):
	"""
	Base consumer with a single queue binding and single channel.
	"""
	def __init__(self, serviceId, host, port):
		self.serviceId = serviceId
		super(RabbitConsumerConnector, self).__init__(host, port)

	def configure(self, exchange, exchangeType, routing_keys=None):
		self.exchange = exchange
		self.channel = self.rabbitConnection.channel()
		self.channel.exchange_declare(exchange=self.exchange, exchange_type=exchangeType)
		self.queue = self.channel.queue_declare(queue=self.serviceId, exclusive=True)
		for key in routing_keys:
			self.channel.queue_bind(exchange=self.exchange, queue=self.queue.method.queue, routing_key=key)
		self.channel.basic_consume(queue=self.queue.method.queue, on_message_callback=self.callback, auto_ack=True)

	def callback(self, ch, method, properties, body):
		pass

	def consume(self):
		pass

	def stopConsumer(self):
		pass


class RabbitProducerConnector(BaseConnector):
	"""
	Base producer connector with dual channel: microservice heartbeat and event dispatcher.
	"""
	def __init__(self, serviceId, host, port):
		self.serviceId = serviceId
		super(RabbitProducerConnector, self).__init__(host, port)

	def configure(self, exchange, exchangeType, routing_keys=None):
		self.exchange = exchange
		self.eventChannel = self.rabbitConnection.channel()
		self.heartbeatChannel = self.rabbitConnection.channel()
		self.eventChannel.exchange_declare(exchange=self.exchange, exchange_type=exchangeType)
		self.heartbeatChannel.exchange_declare(exchange=self.exchange, exchange_type=exchangeType)

	def produce(self):
		pass

	def stopProducer(self):
		pass
