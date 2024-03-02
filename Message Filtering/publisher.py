import pika
import json
import sys
import random

ID = str(sys.argv[1])
msg_ID = ID +'.'+ str(random.randint(1000,10000))
other_inputs = sys.argv[2:]

def group(seq, sep):
    g = []
    for el in seq:
        if el == sep:
            yield g
            g = []
        g.append(el)
    yield g

other_inputs = list(group(other_inputs, 'TASK-ID:'))

tags = other_inputs[0]
msg_body = ' '.join([str(elem) for elem in other_inputs[1]])

msg_num=0

eq_line = "================================================================"
msg_line = "==========================Message==============================="

print(eq_line)
print('[PUBLISHER ' +ID+'] Publishing message')
print(msg_line)
print('Publisher ID:' + ID)
print('Message ID: ' + msg_ID)
print('Message Tag: ' + str(tags))
print('Message body: ' + msg_body)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

message={
    "id": ID,
    "msg_id": msg_ID,
    "tag": str(tags),
    "body": msg_body
}

message = json.dumps(message)

channel.exchange_declare(exchange='task_stream', exchange_type='topic')

for tag in tags:
    channel.basic_publish(
    exchange='task_stream', routing_key=tag, body=message)

print(eq_line)
print('[PUBLISHER ' +ID+'] New message just published')