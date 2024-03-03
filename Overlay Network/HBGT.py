import pika
import json
import random
import sys
import time
import threading

eq_line = "================================================================="
msg_line = "==========================Message==============================="


################Basic Variables #################

refresh = int(sys.argv[1])

network_nodes = [1,4,9,11,14,18,20,21,28]
node_sensors = []

for ID in network_nodes:
    index = network_nodes.index(int(ID))
    try:
        sensors = list(range(int(ID), network_nodes[index+1]))
    except:
        sensors = list(range(int(ID), 32))
        sensors.append(0)
    
    node_sensors.append(sensors)

current_HB = 0
nodes_replied = 0

####################Basic functions##################

def temps_publish(id, message):
    
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='temp_stream', exchange_type='topic')
    

    print('\nHB['+ str(current_HB) +']')
    print(msg_line)
    channel.basic_publish(
        exchange='temp_stream', routing_key=str(id), body=message)

    print("Temps sent to [NODE " + str(id) + ']')
    print(eq_line)

    connection.close()

def acks_subscribe():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='acks_queue', durable=True)
    channel.basic_consume(queue='acks_queue', on_message_callback=ack_callback)

    channel.start_consuming()

def ack_callback(ch, method, properties, body):
    
    global current_HB
    global nodes_replied

    msg = body.decode()

    if len(msg) >0:
        msg = json.loads(msg)
    
        if msg['HB'] == current_HB:
            nodes_replied += 1

        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    global current_HB
    global nodes_replied
####################First Pulse##########################
    temps=[]

    #Generate random temps

    for node in node_sensors:
        node_temps =[]

        for sensor in node:
            node_temps.append(int(random.random() * 400) / 10.0)
        
        temps.append(node_temps)

    #Send temps to nodes
    for index, node_temps in enumerate(temps):
        msg = {
                'id': network_nodes[index],
                'HB': current_HB,
                'temps': node_temps
            }

        msg=json.dumps(msg)

        temps_publish(network_nodes[index], msg)

    time.sleep(10)
        
    while nodes_replied < len(network_nodes):
            pass

    current_HB += 1
    nodes_replied=0

#############Other Pulses##################


    while True:

        if refresh:
            temps=[]

            for node in node_sensors:
                node_temps =[]

                for sensor in node:
                    node_temps.append(int(random.random() * 400) / 10.0)
                
                temps.append(node_temps)

        for index, node_temps in enumerate(temps):
            msg = {
                    'id': network_nodes[index],
                    'HB': current_HB,
                    'temps': node_temps
                }

            msg=json.dumps(msg)

            temps_publish(network_nodes[index], msg)

        
        time.sleep(10)
        
        while nodes_replied < len(network_nodes):
                pass

        current_HB += 1
        nodes_replied=0


sub = threading.Thread(target=acks_subscribe, args=())

sub.start()

main()

     



