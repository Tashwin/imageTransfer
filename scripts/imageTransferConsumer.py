#!/usr/bin/env python
import pika
import base64
import json
import cv2
import numpy as np
import os
import boto
from boto.s3.key import Key
from collections import OrderedDict
import hashlib
import uuid
import argparse

AWS_ACCESS_KEY = ''
AWS_ACCESS_SECRET_KEY = ''
BUCKET_NAME = 'transcriptic-interview'
EXCHANGE_NAME='image_s3_exchange'

def hashfile(afile):
    '''
    Input: file pointer for file to hash
    Output: file specific hashed key
    Description:
        Uses the file data to produce SHA224
        hash key for the file.
    '''
    hasher = hashlib.sha224()
    hasher.update(afile.read())
    key= str(hasher.hexdigest())
    return key

def callback(ch, method, properties, body):
    '''
    Consumer Callback function
    Description:
        1. Reconstructs and rotates the incoming image
        2. Posts the image to s3
        3. Sends the s3 log messages
        4. Sends positive acknowledgement if s3 upload is successful
        else send a negative ack and asks for the msg to be requeued
        5. Rejects an invalid message
    '''

    data = json.loads(body)

    # check keys amd reject if not all required keys are present
    if not isinstance(data,dict) or not all (k in data.keys() for k in ["warp_id","data_type","data"]):
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue =False)

    print "Processing Image Recieved and uploading to S3"

    #Construct cv2 image from the data recieved
    img_mat = getImgFromMsg(data["data"])

    #Rotate image by 180 degrees
    rotated_img = rotateImgBy180(img_mat)

    #Create a uniquely named tmp file
    fname = '/tmp/'+ str(uuid.uuid1()) + '.png'
    #Write to temp file to upload
    cv2.imwrite(fname,rotated_img)

    f = open(fname, 'rb')

    #Creating a hash key based on the file data and append file extension(.png)
    key = hashfile(f) + '.png'

    # Try uploading to S3
    if upload_to_s3(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY, f, BUCKET_NAME, key):

        print 'The rotated image is uploaded'
        # Publish s3 log messages
        s3Channel = ch.connection.channel()
        msgDict = OrderedDict()
        msgDict["warp_id"]= data["warp_id"]
        msgDict["data_type"]= data["data_type"]
        msgDict["s3_info"]= OrderedDict([("bucket", BUCKET_NAME), ("key", key)])
        msg = json.dumps(msgDict)
        s3Channel.basic_publish(exchange=EXCHANGE_NAME,
                routing_key='s3_logs',
                body=msg)
        #Send acknowledment
        ch.basic_ack(delivery_tag = method.delivery_tag)
    else:
        print 'The upload failed...'
        #if s3 upload fails, reque the message
        ch.basic_nack(delivery_tag = method.delivery_tag)
    #Delete the temp file
    os.remove(os.path.join(f.name))


def getImgFromMsg(msg):
    '''
    Input: Base 64 encoded image data
    Output: CV image
    Description:
        Convert Base 64 encoded data to cv image
    '''
    rawImg = base64.b64decode(msg)
    file_bytes = np.asarray(bytearray(rawImg), dtype=np.uint8)
    iMat = cv2.imdecode(file_bytes, cv2.CV_LOAD_IMAGE_UNCHANGED)
    return iMat

def rotateImgBy180(img):
    '''
    Input: Original CV image
    Output: Rotated CV image
    Description:
        Rotates image by 180 degrees
    '''
    rows,cols, ch = img.shape
    M = cv2.getRotationMatrix2D((cols/2,rows/2),180,1)
    dst = cv2.warpAffine(img,M,(cols,rows))
    return dst

def upload_to_s3(aws_access_key_id, aws_secret_access_key, f, bucket_name, key):
    '''
    Input:
        aws_access_key_id: AWS acsess key
        aws_secret_access_key: AWS secret key
        f: file pointer for file to be uploaded
        bucket_name: name of the bucket to upload the file in
        key: key for the file
    Output: Bool indicating failure or success of the upload
    Description:
        Uploads to S3 and returns boolean indicating success/failure of upload.
    '''
    try:
        size = os.fstat(f.fileno()).st_size
    except:
        f.seek(0, os.SEEK_END)
        size = f.tell()
    try:

        conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
        bucket = conn.lookup(bucket_name)
        if not bucket:
            bucket= conn.create_bucket(bucket_name)
        bucket_location = bucket.get_location()
        if bucket_location:
                conn = boto.s3.connect_to_region(bucket_location,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        calling_format = boto.s3.connection.OrdinaryCallingFormat())
                bucket = conn.get_bucket(bucket_name)

        k = bucket.new_key(key)
        sent = k.set_contents_from_file(f, rewind=True)

        if sent == size:
            return True
        return False
    except Exception as e:
        print "S3 Upload Error: ", str(e)
        return False

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Input to the Consumer.')
    requiredNamed = parser.add_argument_group('required arguments')

    requiredNamed.add_argument('-a','--aws_access_key',  type=str,
            required=True,
            help='Amazon s3 access key')
    requiredNamed.add_argument('-s', '--aws_secret_key', type=str,
            required=True,
            help='Amazon s3 secret key')
    args = parser.parse_args()

    AWS_ACCESS_KEY = args.aws_access_key
    AWS_ACCESS_SECRET_KEY = args.aws_secret_key

    #Setup Consumer
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))

    channel = connection.channel()

    #Binding key for image transfer
    binding_key = 'image_queue'

    # Declare the topic exchange for images
    channel.exchange_declare(exchange=EXCHANGE_NAME, type='topic')

    # Create a queue exclusive to each time consumer is getting invoked
    # and bind the exchange and the queue
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange=EXCHANGE_NAME,
            queue=queue_name,
            routing_key=binding_key)

    print(' [*] Waiting for messages. To exit press CTRL+C')

    # Handles bottleneck if multiple consumers invoked
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(callback, queue=queue_name)
    try:
        channel.start_consuming()
    except:
        connection.close()

