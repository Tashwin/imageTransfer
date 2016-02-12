#!/usr/bin/env python
import pika
import argparse
import json
import base64
from collections import OrderedDict
import os

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Input to the Publisher.')
    parser.add_argument('-d','--imageDir',  type=str, default='./',
            help='an input directory for images to tranfer')
    parser.add_argument('-i', '--image', type=str, default='',
            help='Individual image transfer')
    args = parser.parse_args()

    dirName = args.imageDir
    imageName = args.image
    # Create the producer to publish images
    connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))

    channel = connection.channel()

    routing_key = 'image_queue'
    exchange_name = 'image_s3_exchange'

    channel.exchange_declare(exchange=exchange_name, type='topic')

    filesToSend=[]
    if imageName == '' and not os.path.exists(imageName) and not imageName.endswith('.png'):
        for filename in os.listdir(dirName):
            if not filename.endswith('.png'):
                continue
            filesToSend.append(dirName +'/'+filename)
    else:
        filesToSend.append(imageName)

    for filename in filesToSend:
        #Defaults...
        message = OrderedDict([("warp_id",1), ("data_type","image_plate")])

        with open(filename, "rb") as image_file:
            message["data"]= base64.b64encode(image_file.read())

        msg = json.dumps(message)

        channel.basic_publish(exchange=exchange_name,
            routing_key=routing_key,
            body=msg,
            properties=pika.BasicProperties(delivery_mode = 2))

    connection.close()
