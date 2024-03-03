import pika
import json
import random
import sys
import time
import threading
import socket

eq_line = "================================================================="
msg_line = "==========================Message==============================="



##################Get ID, Neighbors and Sensors##################
ID = str(sys.argv[1])

network_nodes = [1,4,9,11,14,18,20,21,28]
other_nodes = [node for node in network_nodes if str(node) != ID]
neighbors_num = random.randint(1,8)
neighbors =[]

for _ in range(neighbors_num):
    choice = random.choice(list(set(other_nodes).difference(neighbors)))
    neighbors.append(choice)

neighbors.sort()

index = network_nodes.index(int(ID))
try:
    sensors = list(range(int(ID), network_nodes[index+1]))
except:
    sensors = list(range(int(ID), 32))
    sensors.append(0)


print('[NODE ' + ID + '] ' + 'Started')
print(eq_line)
print('Neighbors: '+ str(neighbors))
print('Sensors: '+ str(sensors))


################Define global variables##################

current_HB = 0
neighbors_replied = 0

local_min_temp = 0
local_avg_temp = 0
local_max_temp = 0

global_min_temp = 999999999999999
global_avg_temp = 0
global_max_temp = 0

################Define basic functions##################

def node_publish(message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', tcp_options={}))
    channel = connection.channel()

    channel.exchange_declare(exchange='node_stream', exchange_type='topic')

    for neighbor in neighbors:
        channel.basic_publish(
            exchange='node_stream', routing_key=str(neighbor), body=message)
    
    connection.close()
        
def node_subscribe():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()


    channel.exchange_declare(exchange='node_stream', exchange_type='topic')

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='node_stream', queue=queue_name, routing_key=ID)

    channel.basic_consume(
        queue=queue_name, on_message_callback=node_callback, auto_ack=True)
    try:
        channel.start_consuming()
    except:
        print('Error in node_stream restarting...')
        node_subscribe()

def node_callback(ch, method, properties, body):
    global local_min_temp, local_avg_temp, local_max_temp, global_min_temp, global_avg_temp, global_max_temp

    msg = body.decode()

    if len(msg) >0:
        msg = json.loads(msg)

        min_temp = msg['min_temp']
        avg_temp = msg['avg_temp']
        max_temp = msg['max_temp']

        old_global_min_temp = global_min_temp
        old_global_max_temp = global_max_temp
        old_global_avg_temp = global_avg_temp

        global_min_temp = min(global_min_temp, min_temp)
        global_avg_temp = (global_avg_temp + avg_temp) / 2.0
        global_max_temp = max(global_max_temp, max_temp)

        if old_global_min_temp!= global_min_temp or old_global_max_temp!= global_max_temp or old_global_avg_temp!= global_avg_temp:
            new_msg ={
                'min_temp': global_min_temp,
                'avg_temp': global_avg_temp,
                'max_temp': global_max_temp
            }

            new_msg = json.dumps(new_msg)

            node_publish(new_msg)

def temps_subscribe():

    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()


    channel.exchange_declare(exchange='temp_stream', exchange_type='topic')

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='temp_stream', queue=queue_name, routing_key=ID)

    channel.basic_consume(
        queue=queue_name, on_message_callback=temps_callback, auto_ack=True)

    channel.start_consuming()

def temps_callback(ch, method, properties, body):

    global local_min_temp, local_avg_temp, local_max_temp, global_min_temp, global_avg_temp, global_max_temp

    msg = body.decode()

    if len(msg) >0:
        msg = json.loads(msg)

        temps = msg['temps']

        local_min_temp = min(temps)
        local_avg_temp = sum(temps)/len(temps)
        local_max_temp = max(temps)

        old_global_min_temp = global_min_temp
        old_global_max_temp = global_max_temp
        old_global_avg_temp = global_avg_temp

        global_min_temp = min(global_min_temp, local_min_temp)
        global_avg_temp = (global_avg_temp + local_avg_temp) / 2.0
        global_max_temp = max(global_max_temp, local_max_temp)

        if old_global_min_temp!= global_min_temp or old_global_max_temp!= global_max_temp or old_global_avg_temp!= global_avg_temp:
            new_msg ={
                'min_temp': global_min_temp,
                'avg_temp': global_avg_temp,
                'max_temp': global_max_temp
            }

            new_msg = json.dumps(new_msg)

            node_publish(new_msg)

        print('\n[NODE ' + ID + '] ' + 'Temps received')
        print(msg_line)
        print('HB[' + str(msg['HB']) +']')
        print(eq_line)
        print('Min Local Temp:'+ str(local_min_temp))
        print('Avg Local Temp:'+ str(local_avg_temp))
        print('Max Local Temp:'+ str(local_max_temp))
        print(eq_line)
        print('Min Global Temp:'+ str(global_min_temp))
        print('Avg Global Temp:'+ str(global_avg_temp))
        print('Max Global Temp:'+ str(global_max_temp))

        print(eq_line)

        msg ={
            "id": ID,
            "HB": msg['HB']
        }

        msg = json.dumps(msg)

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='acks_queue', durable=True)

        channel.basic_publish(
            exchange='',
            routing_key='acks_queue',
            body=msg,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ))
        
        connection.close()

temps_thread = threading.Thread(target=temps_subscribe, args=())

temps_thread.start()

node_subscribe()
