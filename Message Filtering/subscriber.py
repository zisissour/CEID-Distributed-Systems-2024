import pika
import datetime
import json
import sys
import time 

ID = sys.argv[1]
tags = sys.argv[2:]
msg_num=0


eq_line = "================================================================"
msg_line = "==========================Message==============================="

print(eq_line)
print('[SUBSCRIBER ' +ID+'] Waiting for messages with tags in '+ str(tags)+ '. To exit press CTRL+C')
print('\n\n')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


channel.exchange_declare(exchange='task_stream', exchange_type='topic')

result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue

for tag in tags:
    channel.queue_bind(exchange='task_stream', queue=queue_name, routing_key=tag)

def callback(ch, method, properties, body):
    global msg_num
    msg = body.decode()

    if len(msg) >0:
        print(eq_line)
        msg = json.loads(msg)
        print('[SUBSCRIBER ' +ID+'] Message received at ' + str(datetime.datetime.now()))
        print(msg_line)
        print('Publisher ID:' + msg['id'])
        print('Message ID: ' + msg['msg_id'])
        print('Message Tag: ' + msg['tag'])
        print('Message body: ' + msg['body'])
        
        msg_num+=1
        print(eq_line)
        print('[SUBSCRIBER ' +ID+'] Total number of messages received: '+str(msg_num))

        print(eq_line)
        print('Simulated Working for 5 seconds...')
        time.sleep(5)


channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()