import pika,json

RABBIT_QUEUE_NAME = 'AMBAR_PIPELINE_QUEUE_5cee7571930720542de954c6'
rabbitConnection = pika.BlockingConnection(pika.URLParameters('{0}?heartbeat={1}'.format('amqp://3.16.252.36:5672', 0)))
global rabbitChannel
rabbitChannel = rabbitConnection.channel()
rabbitChannel.basic_qos(prefetch_count=1)

global count
count = 0

def test():
    if count  > 2:
        print('switch queue')
        global rabbitChannel
        rabbitChannel.stop_consuming()
        print('queue stopped')
        rabbitConnection = pika.BlockingConnection(
        pika.URLParameters('{0}?heartbeat={1}'.format('amqp://3.16.252.36:5672', 0)))
        rabbitChannel = rabbitConnection.channel()
        rabbitChannel.basic_qos(prefetch_count=1)
        RABBIT_QUEUE_NAME = 'AMBAR_PIPELINE_QUEUE_5cee36cd81c3792f82b92e6c'
        rabbitChannel.basic_consume(on_message_callback=RabbitConsumeCallback, queue=RABBIT_QUEUE_NAME)
        rabbitChannel.start_consuming()

def RabbitConsumeCallback(channel, method, properties, body):
    message = body
    print('yyyy ', method.delivery_tag)
    print('message ___',message )
    global count
    count = count + 1
    print("count ",count)
    # print(body)
    channel.basic_ack(delivery_tag=method.delivery_tag)
    test()
    print("!!!!!")


rabbitChannel.basic_consume(on_message_callback=RabbitConsumeCallback, queue=RABBIT_QUEUE_NAME)

print("start consuming")
rabbitChannel.start_consuming()












