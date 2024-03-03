import pika
import json
import sys
import random
import time

ID = str(sys.argv[1])
other_inputs = sys.argv[2:]

# Separate tags and task content
tags = [arg for arg in other_inputs if arg.startswith('TASK-ID:')]
msg_body = ' '.join([arg for arg in other_inputs if not arg.startswith('TASK-ID:')])

msg_ID = ID + '.' + str(random.randint(1000, 10000))

eq_line = "================================================================"
msg_line = "==========================Message==============================="

print(eq_line)
print(f'[PUBLISHER {ID}] Publishing message')
print(msg_line)
print('Publisher ID:' + ID)
print('Message ID: ' + msg_ID)
print('Message Tag: ' + str(tags))
print('Message body: ' + msg_body)

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    message = {
        "id": ID,
        "msg_id": msg_ID,
        "tag": str(tags),
        "body": msg_body,
    }

    message = json.dumps(message)

    channel.exchange_declare(exchange='task_stream', exchange_type='topic')

    for tag in tags:
        channel.basic_publish(exchange='task_stream', routing_key=tag, body=message)

    print(eq_line)
    print(f'[PUBLISHER {ID}] New message just published')

    # Adding a delay before closing the connection
    time.sleep(2)
finally:
    connection.close()









