import os
import time
import redis
import logging

from typing import Literal
from redis.exceptions import ConnectionError, TimeoutError
from redis.sentinel import Sentinel
from rediscluster import RedisCluster


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

setup_logging(logging.ERROR)


class RedisDB:
    def __init__(
            self,
            ip_ports=None,
            db=None,
            user_pass=None,
            url=None,
            decode_responses:Literal[True] = True,
            service_name=None,
            max_connections=1000,
            **kwargs
    ):
        """

        :param ip_ports: ip1:port1,ip2:port2
        :param db:
        :param user_pass:
        :param url:
        :param decode_responses:
        :param service_name: redis哨兵模式
        :param max_connections: 同一个redis对象使用的并发数
        :param kwargs:
        """
        if ip_ports is None:
            ip_ports = os.getenv("REDISDB_IP_PORTS")
        if db is None:
            db = os.getenv("REDISDB_DB")
        if user_pass is None:
            user_pass = os.getenv("REDISDB_USER_PASS")
        if service_name is None:
            service_name = os.getenv("REDISDB_SERVICE_NAME")
        if kwargs is None:
            kwargs = os.getenv("REDISDB_KWARGS")

        self._is_redis_cluster = False

        self.__redis = None
        self._url = url
        self._ip_ports = ip_ports
        self._db = db
        self._user_pass = user_pass
        self._decode_responses = decode_responses
        self._service_name = service_name
        self._max_connections = max_connections
        self._kwargs = kwargs
        self.get_connect()

    @property
    def _redis(self):
        try:
            if not self.__redis.ping():
                raise ConnectionError("redis 连接丢失")
        except Exception as e:
            logging.warning(e)
            self._reconnect()

        return self.__redis

    @_redis.setter
    def _redis(self, val):
        self.__redis = val

    def _reconnect(self):
        # 检测连接状态, 当数据库重启或设置 timeout 导致断开连接时自动重连
        retry_count = 0
        while True:
            try:
                retry_count += 1
                logging.error(f"redis 连接断开, 重新连接 {retry_count}")
                if self.get_connect():
                    logging.info(f"redis 连接成功")
                    return True
            except (ConnectionError, TimeoutError) as e:
                logging.error(f"连接失败 e: {e}")

            time.sleep(2)

    def get_connect(self):
        try:
            if not self._ip_ports:
                raise ConnectionError("未设置 Redis 连接信息")

            ip_ports = (
                self._ip_ports
                if isinstance(self._ip_ports, list)
                else self._ip_ports.split(",")
            )
            # 多节点
            if len(ip_ports) > 1:
                startup_nodes = []
                for ip_port in ip_ports:
                    ip, port = ip_port.split(":")
                    startup_nodes.append({"host": ip, "port": int(port)})

                # redis哨兵模式
                if self._service_name:
                    hosts = [(node["host"], node["port"]) for node in startup_nodes]
                    sentinel = Sentinel(hosts, socket_timeout=3, **self._kwargs)
                    self._redis = sentinel.master_for(
                        self._service_name,
                        password=self._user_pass,
                        db=self._db,
                        redis_class=redis.StrictRedis,
                        decode_responses=self._decode_responses,
                        max_connections=self._max_connections,
                        **self._kwargs,
                    )
                # 集群模式
                else:
                    self._redis = RedisCluster(
                        startup_nodes=startup_nodes,
                        decode_responses=self._decode_responses,
                        password=self._user_pass,
                        max_connections=self._max_connections,
                        **self._kwargs,
                    )

                self._is_redis_cluster = True
            # 单节点
            else:
                ip, port = ip_ports[0].split(":")
                self._redis = redis.StrictRedis(
                    host=ip,
                    port=int(port),
                    db=int(self._db),
                    password=self._user_pass,
                    decode_responses=self._decode_responses,
                    max_connections=self._max_connections,
                    **self._kwargs,
                )

                self._is_redis_cluster = False


        except Exception as e:
            logging.error(e)
            raise

        return self.__redis.ping()

    def sget_count(self, table):
        return self._redis.scard(table)

    def lget_count(self, table):
        return self._redis.llen(table)

    def sadd(self, table, values):
        """
        无序set存储数据
        :param table:
        :param values: 支持list 或 单个值
        :return: 0: 存在，1: 入库，None: 批量入库
        """

        if isinstance(values, list):
            pipe = self._redis.pipeline()

            if not self._is_redis_cluster:
                pipe.multi()
            for value in values:
                pipe.sadd(table, value)
            pipe.execute()

        else:
            return self._redis.sadd(table, values)

    def sget(self, table, count=1, is_pop=True):
        """
        无序set随机弹出元素
        :param table:
        :param count:   弹出元素个数
        :param is_pop:  True: 从集合中移除, False: 不从集合中移除
        :return: list
        """
        datas = []
        if is_pop:
            count = count if count <= self.sget_count(table) else self.sget_count(table)
            if count:
                if count > 1:
                    pipe = self._redis.pipeline()

                    if not self._is_redis_cluster:
                        pipe.multi()
                    while count:
                        pipe.spop(table)
                        count -= 1
                    datas = pipe.execute()

                else:
                    datas.append(self._redis.spop(table))

        else:
            datas = self._redis.srandmember(table, count)

        return datas

    def srem(self, table, values):
        """
        移除无序set中的指定元素
        :param table:
        :param values: 一个或者列表
        :return:
        """
        if isinstance(values, list):
            pipe = self._redis.pipeline()

            if not self._is_redis_cluster:
                pipe.multi()
            for value in values:
                pipe.srem(table, value)
            pipe.execute()
        else:
            self._redis.srem(table, values)

    def sdelete(self, table):
        """
        删除无序set (防止阻塞)
        :param table:
        :return:
        """
        cursor = "0"
        while cursor != "0":
            cursor, data = self._redis.sscan(table, cursor=cursor, count=500)

            pipe = self._redis.pipeline()
            for item in data:
                pipe.srem(table, item)  # 将每个删除操作放入管道中

            pipe.execute()

    def sismember(self, table, key):
        """
        是否存在于无序set中
        :param table:
        :param key:
        :return:
        """
        return self._redis.sismember(table, key)

    def lpush(self, table, values):
        """
        插入到 Redis 列表的左端
        :param table:
        :param values: 单个元素或元素列表
        :return:
        """
        if isinstance(values, list):
            pipe = self._redis.pipeline()

            if not self._is_redis_cluster:
                pipe.multi()
            for value in values:
                pipe.lpush(table, value)
            pipe.execute()

        else:
            return self._redis.lpush(table, values)

    def lpop(self, table, count=1):
        """
        列表的左端弹出元素（或多个元素）
        :param table:
        :param count: 要弹出元素的数量
        :return: count>1时返回列表
        """

        datas = None
        lcount = self.lget_count(table)
        count = count if count <= lcount else lcount

        if count:
            if count > 1:
                pipe = self._redis.pipeline()

                if not self._is_redis_cluster:
                    pipe.multi()
                while count:
                    pipe.lpop(table)
                    count -= 1
                datas = pipe.execute()

            else:
                datas = self._redis.lpop(table)

        return datas

    def rpoplpush(self, from_table, to_table=None):
        """
        from_table != to_table:
            from_table 弹出的元素插入到列表 to_table，作为 to_table 列表的头元素
        from_table == to_table:
            列表中的表尾元素被移动到表头，并返回该元素
        :param from_table:
        :param to_table:
        :return: 弹出的元素
        """
        if not to_table:
            to_table = from_table

        return self._redis.rpoplpush(from_table, to_table)

    def lrem(self, table, value, num=0):
        """
        列表中删除指定元素
        :param table:
        :param value: 删除的元素值
        :param num: 删除的数量
        :return: 返回删除的元素数量
        """
        return self._redis.lrem(table, num, value)

    def lrange(self, table, start=0, end=-1):
        """
        获取 Redis 列表中的一个子范围
        :param table:
        :param start: 起始索引
        :param end: 结束索引
        :return: 指定范围内的元素列表
        """
        return self._redis.lrange(table, start, end)

    def set_expire(self, key, seconds):
        """
        设置过期时间
        :param key:
        :param seconds: 秒
        :return:
        """
        self._redis.expire(key, seconds)

    def get_expire(self, key):
        """
        查询过期时间
        :param key:
        :return: 秒
        """
        return self._redis.ttl(key)

    def publish(self, channel, message):
        """
        发布消息到频道
        :param channel: 频道名
        :param message: 消息内容
        :return: 接收该消息的订阅者数量
        """
        return self._redis.publish(channel, message)

    def subscribe(self, channels, handler=None):
        """
        订阅频道
        :param channels: 频道名或频道名列表
        :param handler: 收到消息后的回调函数，格式 handler(message)
        :return: None
        """
        pubsub = self._redis.pubsub()

        if isinstance(channels, str):
            channels = [channels]

        pubsub.subscribe(*channels)

        logging.info(f"订阅频道: {channels}")

        for message in pubsub.listen():
            if message['type'] == 'message':
                if handler:
                    handler(message)
                else:
                    logging.info(f"接收到频道 {message['channel']} 的消息: {message['data']}")

