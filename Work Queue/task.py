import pika
import datetime
import random
import json

ID = str(random.randint(1000,10000))
message_body = "NEW TASK"
time = str(datetime.datetime.now())

eq_line = "================================================================"
mesg_line = "==========================Message==============================="

message={
    "id": ID,
    "timestamp": time,
    "body": message_body
}

message = json.dumps(message)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks_queue', durable=True)

channel.basic_publish(
    exchange='',
    routing_key='tasks_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent
    ))

print(eq_line)
print("ID: "+ID)
print("Timestamp: "+time)
print(mesg_line)
print(message_body)

connection.close()
