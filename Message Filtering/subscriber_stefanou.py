import pika
import datetime
import json
import sys
import time 

ID = sys.argv[1]
tags = sys.argv[2:]

eq_line = "================================================================"
msg_line = "==========================Message==============================="

print(eq_line)
print(f'[SUBSCRIBER {ID}] Waiting for messages with tags in {tags}. To exit press CTRL+C')
print('\n\n')

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='task_stream', exchange_type='topic')

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    for tag in tags:
        channel.queue_bind(exchange='task_stream', queue=queue_name, routing_key=tag)
        print(f'[SUBSCRIBER {ID}] Binding queue to exchange with routing key: {tag}')

    def callback(ch, method, properties, body):
        global msg_num
        msg = body.decode()

        if len(msg) > 0:
            print(f'[SUBSCRIBER {ID}] Entering callback function...')
            print(eq_line)
            msg = json.loads(msg)
            print(f'[SUBSCRIBER {ID}] Message received at {datetime.datetime.now()}')
            print(msg_line)
            print('Publisher ID:' + msg['id'])
            print('Message ID: ' + msg['msg_id'])
            print('Message Tag: ' + msg['tag'])
            print('Message body: ' + msg['body'])

            msg_num += 1
            print(eq_line)
            print(f'[SUBSCRIBER {ID}] Total number of messages received: {msg_num}')

            print(eq_line)
            print(f'Simulated Working for 5 seconds...')
            time.sleep(5)

            # Manually acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

    print(f'[SUBSCRIBER {ID}] Starting to consume messages...')
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    channel.start_consuming()
finally:
    connection.close()











