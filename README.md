### RabbitMQ ->> Image from localhost to AmazonS3 bucket
Author:
    Tashwin Khurana

Project:
    imageTransfer

Description:
    
    * Encode input images to base64 and publish it using rabbitMQ
    * Subsribe to the published image (RabbitMq), rotate them
    * Upload rotated images to Amazon S3 bucket
    * Produce s3 log msg

Usage:
    
    Producer:

      usage: python imageTransferProducer.py [-h] [-d IMAGEDIR] [-i IMAGE]

      Input to the Publisher.
      
      optional arguments:
        -h, --help            show this help message and exit
        -d IMAGEDIR, --imageDir IMAGEDIR
                              an input directory for images to transfer
        -i IMAGE, --image IMAGE
                              Individual image transfer
  
    Image Consumer and uploader:

        usage: python imageTransferConsumer.py [-h] -a AWS_ACCESS_KEY -s AWS_SECRET_KEY
        
        Input to the Consumer.
        
        optional arguments:
          -h, --help            show this help message and exit
        
        required arguments:
          -a AWS_ACCESS_KEY, --aws_access_key AWS_ACCESS_KEY
                                Amazon s3 access key
          -s AWS_SECRET_KEY, --aws_secret_key AWS_SECRET_KEY
                                Amazon s3 secret key
    S3 logger:
        
        usage: python s3Consumer.py
          

Requirements:
    Listed in requirements.txt
    To install all:

        pip install -r requirements.txt

Discussion:

    * Fault tolerance:
            The consumer send a negative acknowledgement and asks for the message to be requeued in case the 
            S3 upload fails

    * Scalability:
            This is addressed using qos functionality, such that the next message will be sent to the first available consumer,
            instead of sending the messages out in an ordered fashion to each consumer. 
