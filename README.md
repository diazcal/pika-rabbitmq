# pika-rabbitmq

This is a colletion of pika-based consumer and producers for microservices or any other software component that needs to produce and consume at the same time. The consumer/producers connect to a RabbitMQ instance and work in different threads to send and receive messages from/to the broker with a heartbeat signal.

I have successfully used this implementation PoC in microservices projects together with custom message definition: Protobuf, Cap'n Proto, JSON, you name it. 
