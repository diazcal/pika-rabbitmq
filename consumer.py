import logging

from .connectors import RabbitConsumerConnector


class RabbitConsumer(RabbitConsumerConnector):
	"""
	The consumer is bound to a RabbitMq Server and consumes messages from the broker.
	Every new message consumed by the broker will be handled by the callback function.
	"""

	def __init__(self, serviceId, rabbithost, rabbitport):
		self.serviceId = serviceId
		super().__init__(serviceId, rabbithost, rabbitport)

	def callback(self, ch, method, properties, body):
		"""
		Receive a message from the Rabbitmq Queue.
		"""
		logging.info(f"Message received from broker({self.host}:{self.port}): {body}")
		message = body.decode("utf-8")
		self.handler.consumeEvent(message)

	def startConsumer(self):
		logging.info(f"Service {self.serviceId} started consumer")
		self.channel.start_consuming()

	def stopConsumer(self):
		self.channel.stop_consuming()

	def attachHandler(self, handler):
		"""
		Handler to manage the message consumed. Needs to implement the consumeEvent() method.
		"""
		self.handler = handler
