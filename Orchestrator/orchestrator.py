import pika
import json
import random
import time
import sys

# Determine whether to generate orchestrator_id randomly or use provided parameter
if len(sys.argv) > 1:
    orchestrator_id = sys.argv[1]
    if not orchestrator_id.isdigit() or len(orchestrator_id) != 4:
        print("Error: Orchestrator ID must be a 4-digit number.")
        sys.exit(1)
else:
    orchestrator_id = str(random.randint(1000, 9999))

amount_of_tasks = len(sys.argv) - 2  # Calculate the number of tasks based on provided parameters

# Ensure at least one computational time is provided
if amount_of_tasks < 1:
    print("Usage: python orchestrator.py [orchestrator_id] <comp_time_task1> <comp_time_task2> ...")
    sys.exit(1)

# Extract the computational times from command line arguments and validate the range
computational_times = []
for i in range(amount_of_tasks):
    execution_time = int(sys.argv[i + 2])
    if 1 <= execution_time <= 5:
        computational_times.append(execution_time)
    else:
        print(f"Error: Task {i+1} execution time must be between 1 and 5 seconds.")
        sys.exit(1)

eq_line = "================================================================"
mesg_line = "==========================Message==============================="

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks_queue', durable=True)

print(eq_line)
print("Orchestrator ID: " + orchestrator_id)
print(mesg_line)

for i, execution_time in enumerate(computational_times):
    task = {
        "id": orchestrator_id,
        "timestamp": str(time.strftime("%Y-%m-%d %H:%M:%S")),
        "body": f"Orchestrator Task {i+1}",
        "execution_time": execution_time
    }
    message = json.dumps(task)

    channel.basic_publish(
        exchange='',
        routing_key='tasks_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent
        ))

    print(f"Sent Orchestrator Task {i+1} with execution time {execution_time} seconds")

print(eq_line)

connection.close()
