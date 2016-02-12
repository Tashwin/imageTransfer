"""
Consumer for s3 log messages
"""
import pika
import sys

def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))

if __name__ == "__main__":
    print "S3 log consumer created"
    # Listener for s3 logs
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='image_s3_exchange',
                             type='topic')

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='image_s3_exchange',
                           queue=queue_name,
                           routing_key='s3_logs')

    print(' [*] Waiting for logs. To exit press CTRL+C')


    channel.basic_consume(callback,
            queue=queue_name,
            no_ack=True)
    try:
        channel.start_consuming()
    except:
        connection.close()

