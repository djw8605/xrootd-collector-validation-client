import pika
import argparse
import os
from dotenv import load_dotenv
import json

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--remove", "-r", help="Remove the messages retrieved from the queue permamently.  Default: Requeue Messages",
                        action="store_true")
    parser.add_argument("--number", "-n", metavar="COUNT", help="Number of messages to retrieve",
                        default=10, type=int)
    parser.add_argument("--queue", "-q", default="xrd.wlcg-itb", help="Queue to receive messages")

    return parser.parse_args()


class ValidationClient:

    def __init__(self, args):
        self.queue = args.queue
        self.receivedMsgs = 0
        self.targetMsgs = args.number
        self.remove = args.remove
        self.timer_id = 0
        self.last_messages = 0

    def recvMsg(self, channel: pika.channel, method, properties, body):
        output = json.loads(body)
        print(json.dumps(output))
        self.receivedMsgs += 1
        if self.receivedMsgs == self.targetMsgs:
            if self.remove:
                channel.basic_ack(method.delivery_tag, multiple=True)
            else:
                channel.basic_nack(method.delivery_tag, multiple=True, requeue=True)
            channel.stop_consuming()

    def createConnection(self):
        # Load the credentials into pika
        if 'RABBIT_URL' not in os.environ:
            raise Exception("Unable to find RABBIT_URL in environment file, .env")
        
        parameters = pika.URLParameters(os.environ["RABBIT_URL"])
        self.conn = pika.adapters.blocking_connection.BlockingConnection(parameters)

        # Connect to the queue
        self.channel = self.conn.channel()

        # Consume from the queue
        self.channel.basic_consume(self.queue, self.recvMsg)

    def _checkStatus(self):
        """
        Called every X seconds to check the status of the transfer.
        If nothing has happened lately, then kill the connection.
        """

        if self.last_messages == self.receivedMsgs:
            self.channel.stop_consuming()
        else:
            self.last_messages = self.receivedMsgs
            self.timer_id = self.conn.call_later(300, self._checkStatus)

    def start(self):
        self.createConnection()

        # Create a timeout
        self.timer_id = self.conn.call_later(300, self._checkStatus)

        self.channel.start_consuming()
        self.conn.close()

        
        

def main():
    args = parse_args()
    load_dotenv()

    validation_client = ValidationClient(args)
    validation_client.start()
    




if __name__ == "__main__":
    main()


