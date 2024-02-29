import pika
import time
import json
import random
import datetime

ID = str(random.randint(1000,10000))

eq_line = "================================================================"
msg_line = "==========================Message==============================="

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks_queue', durable=True)

print('Worker ID: '+ID)
print('Waiting for messages. To exit press CTRL+C')
print('\n\n')


def callback(ch, method, properties, body):

    msg = body.decode()

    if len(msg) >0:

        msg = json.loads(msg)
        print('Message received at ' + str(datetime.datetime.now()))
        print(msg_line)
        print('Sender ID:' + msg['id'])
        print('Timestap: ' + msg['timestamp'])
        print('Message body: ' + msg['body'])
        print(eq_line)

        print('Working...')
        time.sleep(5)        
        print('Task finished!\n\n')

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='tasks_queue', on_message_callback=callback)

channel.start_consuming()