#!/usr/bin/env python
import pika
import argparse
import json
import base64
from collections import OrderedDict

#parser = argparse.ArgumentParser(description='Input to the Publisher.')
#parser.add_argument('-d','--imageDir',  type=str, default='./'
#                           help='an input directory for images to tranfer')
#parser.add_argument('-i', '--image', type='str', default='',
#                                              help='Individual image transfer')
#
#args = parser.parse_args()

connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='image_transfer',
                                 type='topic')


routing_key = 'image.to.s3'

data = OrderedDict([("wrap_id",1), ("data_type","image_plate")])
print data
with open("../testImages/icw.png", "rb") as image_file:
    data["data"]= base64.b64encode(image_file.read())

msg = json.dumps(data)
channel.basic_publish(exchange='image_transfer',
        routing_key=routing_key,
        body=msg)

#print(" [x] Sent %r:%r" % (routing_key, msg))
connection.close()
