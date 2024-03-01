import pika
import time
import json
import random
import datetime
import sys

# Determine whether to generate worker_id randomly or use provided parameter
if len(sys.argv) > 1:
    worker_id = sys.argv[1]
    if not worker_id.isdigit() or len(worker_id) != 4:
        print("Error: Worker ID must be a 4-digit number.")
        sys.exit(1)
else:
    worker_id = str(random.randint(1000, 9999))

total_execution_time = 0  # To keep track of total execution time

eq_line = "================================================================"
msg_line = "==========================Message==============================="

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks_queue', durable=True)

print(eq_line)
print("Worker ID: " + worker_id)
print("Waiting for messages. To exit press CTRL+C")
print(msg_line)

def print_total_execution_time():
    total_symbols = int(total_execution_time)
    print('Total Execution Time: ', end='')
    for _ in range(total_symbols):
        print('█|', end='')  # Draw a line between each symbol
    print('\r', end='')

def callback(ch, method, properties, body):
    global total_execution_time

    try:
        msg = body.decode()

        if len(msg) > 0:
            msg = json.loads(msg)
            print('Message received at ' + str(datetime.datetime.now()))
            print(msg_line)
            print('Sender ID:' + msg['id'])
            print('Timestamp: ' + msg['timestamp'])
            print('Message body: ' + msg['body'])
            print(eq_line)

            print('Working...')
            execution_time = msg.get('execution_time', random.randint(1, 5))  # Use provided computational time or generate random if not available
            total_execution_time += execution_time

            # Print the total amount of work every second
            for _ in range(int(execution_time)):
                time.sleep(1)
                print_total_execution_time()

            time.sleep(execution_time % 1)  # Handle fractional seconds
            print_total_execution_time()

            print('\nTask finished in {} seconds! Total execution time: {} seconds\n\n'.format(execution_time, total_execution_time))

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except KeyboardInterrupt:
        print('\nWorker interrupted')
        sys.exit(0)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='tasks_queue', on_message_callback=callback)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('\nWorker interrupted')
finally:
    connection.close()





