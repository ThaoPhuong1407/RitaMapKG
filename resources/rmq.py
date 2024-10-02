import pika
import json
import threading
import collections
from utils import get_time_millis, make_id  # Import shared functions

class Rmq:
    def __init__(self, exchange, host='localhost', port=5672):
      self.exchange = exchange
      self.host = host
      self.port = port
      self.cb_function = None
      self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
      self.channel = self.connection.channel()
      self.to_rmq = collections.deque()
      self.connection.add_callback_threadsafe(self.rmq_is_idle)
      self.channel_thread = threading.current_thread()
      self.channel.exchange_declare(exchange=exchange, exchange_type='topic')
      self.queue = self.channel.queue_declare(queue='', exclusive=True)
      self.qname = self.queue.method.queue
      self.done = False

    def print_connection_info(self):
      """Print the connection details such as exchange, host, and port."""
      print(f"Exchange: {self.exchange}, Host: {self.host}, Port: {self.port}")

    def subscribe(self, keys):
      """Subscribe to specific routing keys for incoming messages."""
      for key in keys:
        self.channel.queue_bind(queue=self.qname, exchange=self.exchange, routing_key=key)

    def wait_until_keyboard_interrupt(self):
      """Start the message consumption and wait for a keyboard interrupt to stop."""
      try:
        self.channel.start_consuming()
      except KeyboardInterrupt:
        print("Keyboard interrupt, perhaps Control-C.")
      except pika.exceptions.ConnectionClosed:
        print('Received pika.exceptions.ConnectionClosed')


    def _handle_incoming_message(self, channel, method, properties, body):
      """Internal method to decode and process incoming RMQ messages."""
      message = json.loads(body.decode("utf-8"))
      self.cb_function(message, method.routing_key)

    def wait_for_messages(self, callback):
      """Start listening for incoming messages and process them using the provided callback function."""
      self.print_connection_info()
      print("Listening for incoming messages...")
      self.cb_function = callback
      self.channel.basic_consume(queue=self.qname, on_message_callback=self._handle_incoming_message, auto_ack=True)
      self.wait_until_keyboard_interrupt()

    def _publish_message(self, exchange, routing_key, data, properties=None):
      """Publish a message directly to RMQ, checking for thread consistency."""
      current_thread = threading.current_thread()
      if current_thread.ident != self.channel_thread.ident:
          print(f'WARN: Thread mismatch - Current: {current_thread.ident}, Channel: {self.channel_thread.ident}')
      if properties is None:
          self.channel.basic_publish(exchange, routing_key, data)
      else:
          self.channel.basic_publish(exchange, routing_key, data, properties)

    def _process_pending_messages(self):
      """Process all queued messages waiting to be sent to RMQ."""
      while self.to_rmq:
        msg = self.to_rmq.popleft()
        self._publish_message(msg['exchange'], msg['routing-key'], msg['data'], msg['properties'])

    def rmq_is_idle(self):
      """Callback method to send pending messages whenever RMQ is idle."""
      self._process_pending_messages()
      if not self.done:
        self.connection.add_callback_threadsafe(self.rmq_is_idle)

    def _enque_to_rmq(self, exchange, routing_key, data, properties=None):
      """Enqueue a message to be sent later when RMQ is idle."""
      self.to_rmq.append({'exchange': exchange, 'routing-key': routing_key, 'data': data, 'properties': properties})

    def close_connection(self):
      """Gracefully close the RMQ connection, ensuring all pending messages are processed."""
      print("Closing RMQ connection")
      self._process_pending_messages()
      self.channel.close()        
      self.connection.close()

    def publish_message(self, routing_key, msg):
      """Enqueue a message to be published to RMQ."""
      self._enque_to_rmq(self.exchange, routing_key, json.dumps(msg))
      self._process_pending_messages()