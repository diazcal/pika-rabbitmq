from threading import Thread, Event
from queue import Queue
import os
import logging

import pika
from pika.exceptions import ConnectionClosed

from .connectors import RabbitProducerConnector


class RabbitProducer(RabbitProducerConnector):
	"""
	Multithreaded producer.
	Every channel is handled by a different thread, one for the microservice heartbeat and the other to send events to the broker.

	The producer follows the observer pattern and provides a callback() method to the event logic production. Every new event is store in an event queue and will be managed
	by the corresponding thread.
	"""
	def __init__(self, serviceId, host, ip):
		self._eventQueue = Queue()
		self._stopProducing = Event()
		self._stopEvent = Event()
		self.serviceId = serviceId
		self.eventThread = None
		self.heartbeatThread = None
		super(RabbitProducer, self).__init__(serviceId, host, ip)

	def callback(self, event, routingKey):
		routingKey = f'{self.serviceId}.{routingKey}'
		if not self._stopEvent.isSet():
			self._eventQueue.put([event, routingKey])

	def _eventManager(self):
		while not self._stopProducing.isSet():
			try:
				item = self._eventQueue.get()
				cb = item[0]
				key = item[1]
				if cb == "STOP":
					break

				# Publish Callback
				logging.debug(f"router -> RabbitMQ Broker : {cb}")
				self.eventChannel.basic_publish(exchange=self.exchange, routing_key=key, properties=pika.BasicProperties(content_type="event/json"), body=str(cb.json()))

			except Exception as e:
				raise e

	def _heartbeatManager(self):
		while not self._stopProducing.isSet():
			try:
				self.heartbeatChannel.basic_publish(exchange=self.exchange, routing_key="service.heartbeat", properties=pika.BasicProperties(content_type="event/heartbeat"), body="{'event':'heartbeat'}")
				logging.debug(f"heartbeat!")
				self._stopProducing.wait(timeout=58)

			except ConnectionClosed:
				logging.debug(f"Service heartbeat failure. Exiting.")
				os._exit(0)

	def stopProducer(self):
		self._stopEvent.set()
		self._stopProducing.set()
		self._eventQueue.put(["STOP", None])		# Little trick to securily stop the event thread.
		self.eventThread.join()
		self.heartbeatThread.join()

	def startProducer(self):
		self.eventThread = Thread(target=self._eventManager)
		self.eventThread.start()
		logging.info(f"Service {self.serviceId} started producer")
		self._startHheartbeat()

	def _startHheartbeat(self):
		self.heartbeatThread = Thread(target=self._heartbeatManager)
		self.heartbeatThread.start()
		logging.info(f"Service {self.serviceId} started heartbeat")