import pika
import json
import sys
import random

ID = str(sys.argv[1])
msg_ID = ID +'.'+ str(random.randint(1000,10000))
tag = str(sys.argv[2])
msg_body = sys.argv[3:]


msg_body = ' '.join([str(elem) for elem in msg_body])

eq_line = "================================================================"
msg_line = "==========================Message==============================="

print(eq_line)
print('[PUBLISHER ' +ID+'] Publishing message')
print(msg_line)
print('Publisher ID:' + ID)
print('Message ID: ' + msg_ID)
print('Message Tag: ' + str(tag))
print('Message body: ' + msg_body)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

message={
    "id": ID,
    "msg_id": msg_ID,
    "tag": str(tag),
    "body": msg_body
}

message = json.dumps(message)

channel.exchange_declare(exchange='task_stream', exchange_type='topic')

channel.basic_publish(
    exchange='task_stream', routing_key=tag, body=message)

print(eq_line)
print('[PUBLISHER ' +ID+'] New message just published')