#!/usr/bin/env python
import pika
import sys
import base64
import json
import cv2
import numpy as np

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='image_transfer',
                         type='topic')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

binding_keys = ['image.#']

for binding_key in binding_keys:
    channel.queue_bind(exchange='image_transfer',
                       queue=queue_name,
                       routing_key=binding_key)


def callback(ch, method, properties, body):
    data = json.loads(body)
    img_mat = getImgFromMsg(data["data"])
    rotated_img = rotateImgBy180(img_mat)

def getImgFromMsg(msg):
    rawImg = base64.b64decode(msg)
    file_bytes = np.asarray(bytearray(rawImg), dtype=np.uint8)
    iMat = cv2.imdecode(file_bytes, cv2.CV_LOAD_IMAGE_UNCHANGED)
    return iMat

def rotateImgBy180(img):
    rows,cols, ch = img.shape
    M = cv2.getRotationMatrix2D((cols/2,rows/2),180,1)
    dst = cv2.warpAffine(img,M,(cols,rows))
    return dst


channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()

