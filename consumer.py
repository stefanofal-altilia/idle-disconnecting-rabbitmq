import pika
from pika import ConnectionParameters


def handle(ch, method, properties, body):
    """
    Handle a received Message on a queue. The method execute the action's Callback function and then
    give an ack to the Message.
    :param ch:
    :type ch:
    :param method:
    :type method:
    :param properties:
    :type properties:
    :param body:
    :type body:
    :return: None
    :rtype:
    """
    try:
        print("Method called handle")
        print("Message with id {} arrived".format(properties.correlation_id))
        body = "I recevied a message!"
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                         body=str.encode(body))
        print("Response sended")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("Method ended handle")
    except BaseException as e:
        print(
            "Fatal error on message broker class {} method {} error {}".format("RabbitMQConsumer", "handle",
                                                                               str(e)))


def consume_select_connection():
    print("Method called consume_select_connection")

    def on_open(conn):
        print("Method called on_open")
        conn.channel(on_open_callback=on_channel_open)
        print("Method ended on_open")

    def on_channel_open(channel):
        print("Method called on_channel_open")
        channel.queue_declare("annotation-request-queue", passive=False, durable=True,
                              exclusive=False, auto_delete=False)
        channel.basic_consume("annotation-request-queue", on_message_callback=handle)
        print("Method ended on_channel_open")

    credentials = pika.PlainCredentials(username="guest",
                                        password="guest")
    parameters = ConnectionParameters(
        host="localhost",
        port=5672,
        credentials=credentials,
        blocked_connection_timeout=60)
    connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open)
    try:
        print("I/O start")
        #  Block on the IOLoop
        connection.ioloop.start()

    # Catch a Keyboard Interrupt to make sure that the connection is closed cleanly
    except KeyboardInterrupt:
        print("keyboard exception")
        # Gracefully close the connection
        connection.close()
    except BaseException as e:
        print(("BASE exception: ", str(e)))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    consume_select_connection()
