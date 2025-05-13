import os
import pika
import time
import logging


def setup_logging(level=logging.WARNING, to_file=None):
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers = []

    if to_file:
        handler = logging.FileHandler(to_file)
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

setup_logging(logging.INFO)


class RabbitMqDB:
    def __init__(
        self,
        host=None,
        port=None,
        user=None,
        password=None,
        vhost="/",
        heartbeat=60,
        timeout=300,
        reconnect_attempts=3,
        reconnect_delay=5
    ):
        self.host = host or os.getenv("RABBITMQ_IP")
        self.port = int(port or os.getenv("RABBITMQ_PROT", 5672))
        self.user = user or os.getenv("RABBITMQ_USER")
        self.password = password or os.getenv("RABBITMQ_PASS")
        self.vhost = vhost
        self.heartbeat = heartbeat
        self.timeout = timeout
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_delay = reconnect_delay

        self.connection = None
        self.channel = None

        self._connect()

    def _connect(self):
        for attempt in range(1, self.reconnect_attempts + 1):
            try:
                credentials = pika.PlainCredentials(self.user, self.password)
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=credentials,
                    heartbeat=self.heartbeat,
                    blocked_connection_timeout=self.timeout,
                    virtual_host=self.vhost
                )
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                logging.info("RabbitMQ 连接成功")
                return
            except Exception as e:
                logging.warning(
                    """
                    RabbitMQ 连接失败
                    exception: {}
                    host: {}
                    port: {}
                    user: {}
                    password: {}
                    """.format(e, self.host, self.port, self.user, self.password)
                )
                if attempt < self.reconnect_attempts:
                    time.sleep(self.reconnect_delay)
                else:
                    raise ConnectionError("RabbitMQ 多次连接失败") from e

    def _ensure_connection(self):
        try:
            if not self.connection or self.connection.is_closed:
                logging.warning("RabbitMQ 连接已关闭，尝试重新连接...")
                self._connect()
            elif self.channel is None or self.channel.is_closed:
                logging.warning("Channel 已关闭，重建 Channel")
                self.channel = self.connection.channel()
        except Exception as e:
            logging.error(f"恢复连接失败: {e}")
            self._connect()  # 失败后强制重连

    def declare_exchange(
        self,
        exchange,
        exchange_type='fanout',
        durable=False
    ):
        self._ensure_connection()
        self.channel.exchange_declare(
            exchange=exchange,
            exchange_type=exchange_type,
            durable=durable
        )

    def declare_and_bind_queue(
        self,
        queue='',
        exchange='',
        routing_key='',
        exclusive=True
    ):
        self._ensure_connection()
        result = self.channel.queue_declare(queue=queue, exclusive=exclusive)
        queue_name = result.method.queue
        self.channel.queue_bind(
            exchange=exchange,
            queue=queue_name,
            routing_key=routing_key
        )
        return queue_name

    def send_message(
        self,
        exchange: str = '',
        routing_key: str = '',
        message: str = '',
        exchange_type: str = 'fanout'
    ):
        try:
            self._ensure_connection()
            self.declare_exchange(exchange, exchange_type)
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message.encode() if isinstance(message, str) else message
            )
            logging.info(f"发送消息成功 → exchange: '{exchange}', routing_key: '{routing_key}'")
        except Exception as e:
            logging.error(f"发送消息失败 → exchange: '{exchange}', 错误: {e}", exc_info=True)


    def receive_message(
        self,
        exchange: str = '',
        exchange_type: str = 'fanout',
        routing_key: str = '',
        queue: str = '',
        exclusive: bool = True,
        auto_ack: bool = True,
        callback=None
    ):
        self._ensure_connection()
        self.declare_exchange(exchange, exchange_type)
        queue_name = self.declare_and_bind_queue(queue, exchange, routing_key, exclusive)

        def default_callback(ch, method, properties, body):
            logging.info(f"接收到消息: {body.decode()}")

        logging.info(f"[*] 等待来自 exchange '{exchange}' 的消息，监听队列 '{queue_name}' ...")
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback or default_callback,
            auto_ack=auto_ack
        )
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.info("手动中断 RabbitMQ 消费")
        except Exception as e:
            logging.error("消费消息过程中出错 -> %s" % e, exc_info=True)

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()
            logging.info("RabbitMQ 连接已关闭")
