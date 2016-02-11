#!/usr/bin/env python
import pika
import sys
import base64
import json
import cv2
import numpy as np
import os
import boto
from boto.s3.key import Key



AWS_ACCESS_KEY = 'AKIAJNNW4Q73OEFE4R2A'
#AWS_ACCESS_KEY = 'AKIAI7SHQXA25PXW5PXQ'
AWS_ACCESS_SECRET_KEY = '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'
#AWS_ACCESS_SECRET_KEY = 'ghotn6gfzCk8o+EfKFn9Yxs1c9hPEirveGDYUlt5'
BUCKET_NAME = 'transcriptic-interview-test'

def callback(ch, method, properties, body):
    data = json.loads(body)
    img_mat = getImgFromMsg(data["data"])
    rotated_img = rotateImgBy180(img_mat)
    cv2.imwrite('/tmp/image.png',rotated_img)
    f = open('/tmp/image.png', 'rb')
    key = f.name.split('/')
    if upload_to_s3(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY, f, BUCKET_NAME, key):
        print 'It worked!'
    else:
        print 'The upload failed...'

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

def upload_to_s3(aws_access_key_id, aws_secret_access_key, f, bucket_name, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
    """
    Returns boolean indicating success/failure of upload.
    """
    try:
        size = os.fstat(f.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        f.seek(0, os.SEEK_END)
        size = f.tell()

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = conn.get_bucket(bucket_name)
    bucket_location = bucket.get_location()
    if bucket_location:
            conn = boto.s3.connect_to_region(bucket_location,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    calling_format = boto.s3.connection.OrdinaryCallingFormat())
            bucket = conn.get_bucket(bucket_name)

    k = bucket.new_key(key)
    #if content_type:
     #   k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(f, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)

    # Rewind for later use

    if sent == size:
        return True
    return False

def main():
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

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()

main()
