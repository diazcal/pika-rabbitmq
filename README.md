# pika-rabbitmq

This is a colletion of pika-based consumer and producers for a major microservice migration research project where I am contributing as a Solutions Architect and had to support development with some microservice coding for a PoC.
The consumer/producers connect to a RabbitMQ instance and need to work in parallel to send and receive messages from/to the broker.

### Out of the repo
I left out the folling since is not very interesting to me:
- Consumer handler logic --> any non blocking logic is fine to consume messages.
- Producer event dispatcher --> following the observer pattern is enough
- main.py
