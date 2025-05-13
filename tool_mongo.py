import os
import logging
from typing import List, Dict, Optional
from dotenv import load_dotenv

import pymongo
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, BulkWriteError

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

class MongoDB:
    def __init__(
        self,
        ip=None,
        port=None,
        db=None,
        user_name=None,
        user_pass=None,
        auth_source=None,
        **kwargs,
    ):
        if not ip:
            ip = os.getenv("MONGO_IP")
        if not port:
            port =os.getenv("MONGO_PORT")
        if not db:
            db = os.getenv("MONGO_DB")
        if not user_name:
            user_name = os.getenv("MONGO_USER_NAME")
        if not user_pass:
            user_pass = os.getenv("MONGO_USER_PASS")
        if not auth_source:
            auth_source = os.getenv("MONGO_AUTHSOURCE")
        try:
            self.client = MongoClient(
                host=ip, port=int(port), username=user_name, password=user_pass, authSource=auth_source, **kwargs
            )

            self.db = self.get_database(db)
        except Exception as e:
            logging.error(
                """
            连接失败：
            ip: {}
            port: {}
            db: {}
            user_name: {}
            user_pass: {}
            auth_source: {}
            exception: {}
            """.format(
                    ip, port, db, user_name, user_pass, auth_source, e
                )
            )
        else:
            logging.debug("连接到mongo数据库 %s : %s" % (ip, db))

        self.__index__cached = {}

    def get_database(self, database, **kwargs) -> Database:
        """
        获取数据库对象
        :param database: 数据库名
        :param kwargs:
        :return:
        """

        return self.client.get_database(database, **kwargs)

    def get_collection(self, coll_name, **kwargs) -> Collection:
        """
        根据集合名获取集合对象
        :param coll_name: 集合名
        :param kwargs:
        :return:
        """

        return self.db.get_collection(coll_name, **kwargs)

    def find(
        self, coll_name: str, condition: Optional[Dict] = None, limit: int = 0, **kwargs
    ) -> List[Dict]:
        """
        查询
        :param coll_name: 集合名(表名)
        :param condition: 查询条件
        :param limit: 结果数量
        :param kwargs: 更多参数 https://docs.mongodb.com/manual/reference/command/find/#command-fields
        :return:
            无数据： 返回[]
            有数据： [{'_id': 'xx', ...}, ...]
        """

        condition = {} if condition is None else condition
        command = {"find": coll_name, "filter": condition, "limit": limit}
        command.update(kwargs)
        result = self.run_command(command)
        cursor = result["cursor"]
        cursor_id = cursor["id"]
        dataset = cursor["firstBatch"]
        while True:
            if cursor_id == 0:
                break
            result = self.run_command(
                {
                    "getMore": cursor_id,
                    "collection": coll_name,
                    "batchSize": kwargs.get("batchSize", 100),
                }
            )
            cursor = result["cursor"]
            cursor_id = cursor["id"]
            dataset.extend(cursor["nextBatch"])
        return dataset

    def add(
        self,
        coll_name,
        data: Dict,
        replace=False,
        update_columns=(),
        update_columns_value=(),
        insert_ignore=False,
    ):
        """
        添加单条数据
        :param coll_name: 集合名
        :param data: 单条数据
        :param replace: 唯一索引冲突时直接覆盖旧数据，默认为False
        :param update_columns: 更新指定的列（如果数据唯一索引冲突，则更新指定字段，如 update_columns = ["name", "title"]
        :param update_columns_value: 指定更新的字段对应的值, 不指定则用数据本身的值更新
        :param insert_ignore: 索引冲突是否忽略 默认False
        :return: 插入成功的行数
        """

        affect_count = 1
        collection = self.get_collection(coll_name)
        try:
            collection.insert_one(data)
        except DuplicateKeyError as e:
            # 存在则更新
            if update_columns:
                if not isinstance(update_columns, (tuple, list)):
                    update_columns = [update_columns]

                condition = self.__get_update_condition(
                    coll_name, data, e.details.get("errmsg")
                )

                # 更新指定的列
                if update_columns_value:
                    # 使用指定的值更新
                    doc = {
                        key: value
                        for key, value in zip(update_columns, update_columns_value)
                    }
                else:
                    # 使用数据本身的值更新
                    doc = {key: data[key] for key in update_columns}

                collection.update_one(condition, {"$set": doc})

            # 覆盖更新
            elif replace:
                condition = self.__get_update_condition(
                    coll_name, data, e.details.get("errmsg")
                )
                # 替换已存在的数据
                collection.replace_one(condition, data)

            elif not insert_ignore:
                raise e

        return affect_count

    def add_batch(
        self,
        coll_name: str,
        datas: List[Dict],
        replace=False,
        update_columns=(),
        update_columns_value=(),
        condition_fields: dict = None,
    ):
        """
        批量添加数据
        :param coll_name: 集合名
        :param datas: 数据 [{'_id': 'xx'}, ... ]
        :param replace: 唯一索引冲突时直接覆盖旧数据，默认为False
        :param update_columns: 更新指定的列（如果数据的唯一索引存在，则更新指定字段，如 update_columns = ["name", "title"]
        :param update_columns_value: 指定更新的字段对应的值, 不指定则用数据本身的值更新
        :param condition_fields: 用于条件查找的字段，不指定则用索引冲突中的字段查找
        :return: 添加行数，不包含更新
        """

        add_count = 0

        if not datas:
            return add_count

        collection = self.get_collection(coll_name)
        if not isinstance(update_columns, (tuple, list)):
            update_columns = [update_columns]

        try:
            add_count = len(datas)
            collection.insert_many(datas, ordered=False)
        except BulkWriteError as e:
            write_errors = e.details.get("writeErrors")
            for error in write_errors:
                if error.get("code") == 11000:
                    # 数据重复
                    # 获取重复的数据
                    data = error.get("op")

                    def get_condition():
                        # 获取更新条件
                        if condition_fields:
                            condition = {
                                condition_field: data[condition_field]
                                for condition_field in condition_fields
                            }
                        else:
                            # 根据重复的值获取更新条件
                            condition = self.__get_update_condition(
                                coll_name, data, error.get("errmsg")
                            )

                        return condition

                    if update_columns:
                        # 更新指定的列
                        if update_columns_value:
                            # 使用指定的值更新
                            doc = {
                                key: value
                                for key, value in zip(
                                    update_columns, update_columns_value
                                )
                            }
                        else:
                            # 使用数据本身的值更新
                            doc = {key: data.get(key) for key in update_columns}

                        collection.update_one(get_condition(), {"$set": doc})
                        add_count -= 1

                    elif replace:
                        # 覆盖更新
                        collection.replace_one(get_condition(), data)
                        add_count -= 1

                    else:
                        logging.error(error)
                        add_count -= 1

        return add_count

    def count(self, coll_name, condition: Optional[Dict], limit=0, **kwargs):
        """
        计数
        :param coll_name: 集合名
        :param condition: 查询条件
        :param limit: 限制数量
        :param kwargs:
         ----
        command = {
          count: <collection or view>,
          query: <document>,
          limit: <integer>,
          skip: <integer>,
          hint: <hint>,
          readConcern: <document>,
          collation: <document>,
          comment: <any>
        }
        https://docs.mongodb.com/manual/reference/command/count/#mongodb-dbcommand-dbcmd.count
        :return: 数据数量
        """

        command = {"count": coll_name, "query": condition, "limit": limit, **kwargs}
        result = self.run_command(command)
        return result["n"]

    def update(self, coll_name, data: Dict, condition: Dict, upsert: bool = False):
        """
        更新
        :param coll_name: 集合名
        :param data: 单条数据 {"xxx":"xxx"}
        :param condition: 更新条件 {"_id": "xxxx"}
        :param upsert: 数据不存在则插入,默认为 False
        :return: True / False
        """

        try:
            collection = self.get_collection(coll_name)
            collection.update_one(condition, {"$set": data}, upsert=upsert)
        except Exception as e:
            logging.error(
                """
                error:{}
                condition: {}
            """.format(
                    e, condition
                )
            )
            return False
        else:
            return True

    def update_many(self, coll_name, data: Dict, condition: Dict, upsert: bool = False):
        """
        批量更新
        :param coll_name: 集合名
        :param data: 单条数据 {"xxx":"xxx"}
        :param condition: 更新条件 {"_id": "xxxx"}
        :param upsert: 数据不存在则插入,默认为 False
        :return: True / False
        """

        try:
            collection = self.get_collection(coll_name)
            collection.update_many(condition, {"$set": data}, upsert=upsert)
        except Exception as e:
            logging.error(
                """
                error:{}
                condition: {}
            """.format(
                    e, condition
                )
            )
            return False
        else:
            return True

    def update_batch(
        self,
        coll_name: str,
        update_data_list: List[Dict],
        condition_field: str,
        upsert: bool = False,
    ):
        """
        批量更新数据
        :param coll_name: 集合名
        :param update_data_list: 更新数据列表
        :param condition_field: 更新条件字段
        :param upsert: 数据不存在则插入，默认为 False
        :return: 更新行数
        """

        if not update_data_list:
            return 0

        collection = self.get_collection(coll_name)
        bulk_operations = []

        for update_data in update_data_list:
            condition = {condition_field: update_data.get(condition_field)}
            update_operation = UpdateOne(
                condition, {"$set": update_data}, upsert=upsert
            )
            bulk_operations.append(update_operation)
        try:
            result = collection.bulk_write(bulk_operations, ordered=False)
            return result.modified_count + result.upserted_count
        except BulkWriteError as e:
            logging.error(f"Bulk write error: {e.details}")
            return 0

    def delete(self, coll_name, condition: Dict) -> bool:
        """
        删除
        :param coll_name: 集合名
        :param condition: 查找条件
        :return: True / False
        """

        try:
            collection = self.get_collection(coll_name)
            collection.delete_one(condition)
        except Exception as e:
            logging.error(
                """
                error:{}
                condition: {}
            """.format(
                    e, condition
                )
            )
            return False
        else:
            return True

    def run_command(self, command: Dict):
        """
        运行指令
        https://www.geek-book.com/src/docs/mongodb/mongodb/docs.mongodb.com/manual/reference/command/index.html
        :param command:
        :return:
        """

        return self.db.command(command)

    def create_index(self, coll_name, keys, unique=True):
        collection = self.get_collection(coll_name)
        _keys = [(key, pymongo.ASCENDING) for key in keys]
        collection.create_index(_keys, unique=unique)

    def get_index(self, coll_name):
        return self.get_collection(coll_name).index_information()

    def drop_collection(self, coll_name):
        return self.db.drop_collection(coll_name)

    def __getattr__(self, name):
        return getattr(self.db, name)


