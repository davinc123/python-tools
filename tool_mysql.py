import os
import re
import json
import pymysql
import logging
import datetime

from pprint import pformat
from dotenv import load_dotenv
from pymysql import err, cursors
from typing import List, Dict, Any
from dbutils.pooled_db import PooledDB


load_dotenv(dotenv_path=f".env")

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

_regexs = {}
def get_info(html, regexs, allow_repeat=True, fetch_one=False, split=None):
    regexs = isinstance(regexs, str) and [regexs] or regexs

    infos = []
    for regex in regexs:
        if regex == "":
            continue

        if regex not in _regexs.keys():
            _regexs[regex] = re.compile(regex, re.S)

        if fetch_one:
            infos = _regexs[regex].search(html)
            if infos:
                infos = infos.groups()
            else:
                continue
        else:
            infos = _regexs[regex].findall(str(html))

        if len(infos) > 0:
            break

    if fetch_one:
        infos = infos if infos else ("",)
        return infos if len(infos) > 1 else infos[0]
    else:
        infos = allow_repeat and infos or sorted(set(infos), key=infos.index)
        infos = split.join(infos) if split else infos
        return infos


def get_json(json_str):
    try:
        return json.loads(json_str) if json_str else {}
    except Exception as e1:
        try:
            json_str = json_str.strip()
            json_str = json_str.replace("'", '"')
            keys = get_info(json_str, "(\w+):")
            for key in keys:
                json_str = json_str.replace(key, '"%s"' % key)

            return json.loads(json_str) if json_str else {}

        except Exception as e2:
            log.error(
                """
                e1: %s
                format json_str: %s
                e2: %s
                """
                % (e1, json_str, e2)
            )

        return {}


def dumps_json(data, indent=4, sort_keys=False):
    try:
        if isinstance(data, str):
            data = get_json(data)

        data = json.dumps(
            data,
            ensure_ascii=False,
            indent=indent,
            skipkeys=True,
            sort_keys=sort_keys,
            default=str,
        )

    except Exception as e:
        log.error(
            """
            e: %s
            data: %s
            """
            % (e, data)
        )
        data = pformat(data)

    return data


def format_sql_value(value):
    if isinstance(value, str):
        value = value.strip()

    elif isinstance(value, (list, dict)):
        value = dumps_json(value)

    elif isinstance(value, (datetime.date, datetime.time)):
        value = str(value)

    elif isinstance(value, bool):
        value = int(value)

    return value


def list2str(datas):
    data_str = str(tuple(datas))
    data_str = re.sub(",\)$", ")", data_str)
    return data_str


def make_insert_sql(
    table, data, auto_update=False, update_columns=(), insert_ignore=False
):
    """
    to_date 处理
    :param table:
    :param data: 表数据 json格式
    :param auto_update: 使用的是replace into， 为完全覆盖已存在的数据
    :param update_columns: 需要更新的列 默认全部，当指定值时，auto_update设置无效，当duplicate key冲突时更新指定的列
    :param insert_ignore: 数据存在忽略
    :return:
    """

    keys = ["`{}`".format(key) for key in data.keys()]
    keys = list2str(keys).replace("'", "")

    values = [format_sql_value(value) for value in data.values()]
    values = list2str(values)

    if update_columns:
        if not isinstance(update_columns, (tuple, list)):
            update_columns = [update_columns]
        update_columns_ = ", ".join(
            ["{key}=values({key})".format(key=key) for key in update_columns]
        )
        sql = (
            "insert%s into `{table}` {keys} values {values} on duplicate key update %s"
            % (" ignore" if insert_ignore else "", update_columns_)
        )

    elif auto_update:
        sql = "replace into `{table}` {keys} values {values}"
    else:
        sql = "insert%s into `{table}` {keys} values {values}" % (
            " ignore" if insert_ignore else ""
        )

    sql = sql.format(table=table, keys=keys, values=values).replace("None", "null")
    return sql


def make_batch_sql(
    table, datas, auto_update=False, update_columns=(), update_columns_value=()
):
    """
    生产批量的sql
    :param table:
    :param datas: 表数据 [{...}]
    :param auto_update: 使用的是replace into， 为完全覆盖已存在的数据
    :param update_columns: 需要更新的列 默认全部，当指定值时，auto_update设置无效，当duplicate key冲突时更新指定的列
    :param update_columns_value: 需要更新的列的值 默认为datas里边对应的值, 如果值为字符串类型 需要主动加单引号， 如 update_columns_value=("'test'",)
    :return:
    """
    if not datas:
        return

    keys = list(set([key for data in datas for key in data]))
    values_placeholder = ["%s"] * len(keys)

    values = []
    for data in datas:
        value = []
        for key in keys:
            current_data = data.get(key)
            current_data = format_sql_value(current_data)

            value.append(current_data)

        values.append(value)

    keys = ["`{}`".format(key) for key in keys]
    keys = list2str(keys).replace("'", "")

    values_placeholder = list2str(values_placeholder).replace("'", "")

    if update_columns:
        if not isinstance(update_columns, (tuple, list)):
            update_columns = [update_columns]
        if update_columns_value:
            update_columns_ = ", ".join(
                [
                    "`{key}`={value}".format(key=key, value=value)
                    for key, value in zip(update_columns, update_columns_value)
                ]
            )
        else:
            update_columns_ = ", ".join(
                ["`{key}`=values(`{key}`)".format(key=key) for key in update_columns]
            )
        sql = "insert into `{table}` {keys} values {values_placeholder} on duplicate key update {update_columns}".format(
            table=table,
            keys=keys,
            values_placeholder=values_placeholder,
            update_columns=update_columns_,
        )
    elif auto_update:
        sql = "replace into `{table}` {keys} values {values_placeholder}".format(
            table=table, keys=keys, values_placeholder=values_placeholder
        )
    else:
        sql = "insert ignore into `{table}` {keys} values {values_placeholder}".format(
            table=table, keys=keys, values_placeholder=values_placeholder
        )

    return sql, values


def make_update_sql(table, data, condition):
    """
    to_date 处理
    :param table:
    :param data: 表数据 json格式
    :param condition: where 条件
    :return:
    """
    key_values = []

    for key, value in data.items():
        value = format_sql_value(value)
        if isinstance(value, str):
            key_values.append("`{}`={}".format(key, repr(value)))
        elif value is None:
            key_values.append("`{}`={}".format(key, "null"))
        else:
            key_values.append("`{}`={}".format(key, value))

    key_values = ", ".join(key_values)

    sql = "update `{table}` set {key_values} where {condition}"
    sql = sql.format(table=table, key_values=key_values, condition=condition)
    return sql


class MysqlDB:
    def __init__(
        self, ip=None, port=None, db=None, user_name=None, user_pass=None
    ):
        if not ip:
            ip = os.getenv("MYSQL_IP")
        if not port:
            port = os.getenv("MYSQL_PORT")
        if not db:
            db = os.getenv("MYSQL_DB")
        if not user_name:
            user_name = os.getenv("MYSQL_USER_NAME")
        if not user_pass:
            user_pass = os.getenv("MYSQL_USER_PASS")
        try:
            self.connect_pool = PooledDB(
                creator=pymysql,
                mincached=1,
                maxcached=100,
                maxconnections=100,
                blocking=True,
                ping=7,
                host=ip,
                port=int(port),
                user=user_name,
                passwd=user_pass,
                db=db,
                charset="utf8mb4",
                cursorclass=cursors.SSCursor,
            )

        except Exception as e:
            log.error(
                """
            连接失败：
            ip: {}
            port: {}
            db: {}
            user_name: {}
            user_pass: {}
            exception: {}
            """.format(
                    ip, port, db, user_name, user_pass, e
                )
            )
        else:
            log.debug("连接到mysql数据库 %s : %s" % (ip, db))

    def get_connection(self):
        conn = self.connect_pool.connection(shareable=False)
        cursor = conn.cursor()

        return conn, cursor

    def close_connection(self, conn, cursor):
        if conn:
            conn.close()
        if cursor:
            cursor.close()

    def find(self, sql, params=None, limit=0, to_json=False, conver_col=True):
        """
        查询
        :param sql:
        :param params:
        :param limit:
        :param to_json: 是否将查询结果转为json
        :param conver_col: 是否处理查询结果，如date类型转字符串，json字符串转成json。
        :return:
        """
        conn, cursor = self.get_connection()

        cursor.execute(sql, params or ())

        if limit == 1:
            result = cursor.fetchone()
        elif limit > 1:
            result = cursor.fetchmany(limit)
        else:
            result = cursor.fetchall()

        if to_json:
            columns = [i[0] for i in cursor.description]

            def convert(col):
                if isinstance(col, (datetime.date, datetime.time)):
                    return str(col)
                elif isinstance(col, str) and (
                        col.startswith("{") or col.startswith("[")
                ):
                    try:
                        return json.loads(col)
                    except Exception as e:
                        log.error(
                            """
                        处理失败：
                        exception: {}
                        """.format(
                                 e
                            )
                        )
                        return col
                else:
                    return col
            if limit == 1:
                if conver_col:
                    result = [convert(col) for col in result]
                result = dict(zip(columns, result))
            else:
                if conver_col:
                    result = [[convert(col) for col in row] for row in result]
                result = [dict(zip(columns, r)) for r in result]

        self.close_connection(conn, cursor)

        return result

    def add(self, sql, params=None,exception_callfunc=None):
        """
        执行 SQL 语句（支持参数化），自动提交，插入时返回自增 ID。

        :param sql: SQL 语句
        :param params: SQL 参数（元组或列表）
        :param exception_callfunc: 异常回调函数（可选）
        :return: 插入后的自增 ID（int）
        """
        conn, cursor = None, None

        try:
            conn, cursor = self.get_connection()
            cursor.execute(sql, params or ())
            conn.commit()

        except Exception as e:
            log.error(
                """
                error:%s
                sql:  %s
            """
                % (e, sql)
            )
            if exception_callfunc:
                exception_callfunc(e)
        finally:
            self.close_connection(conn, cursor)

        return cursor.lastrowid

    def add_smart(self, table, data, **kwargs):
        """
        添加数据，直接传递json格式的数据。
        :param table:
        :param data:
        :param kwargs:
        :return:
        """
        sql = make_insert_sql(table, data, **kwargs)
        return self.add(sql)

    def add_batch(self, sql, datas: List[List]):
        """
        批量添加数据
        :param sql: insert ignore into (xxx,xxx,xxx) values (%s, %s, %s)
        :param datas: 列表 [[v1,v2,v3], [v1,v2,v3]]
        :return: 添加行数
        """
        affect_count = None
        conn, cursor = None, None

        try:
            conn, cursor = self.get_connection()
            affect_count = cursor.executemany(sql, datas)
            conn.commit()

        except Exception as e:
            log.error(
                """
                error:%s
                sql:  %s
                """
                % (e, sql)
            )
        finally:
            self.close_connection(conn, cursor)

        return affect_count

    def add_batch_smart(self, table, datas: List[Dict], **kwargs) -> int:
        """
        批量添加数据, 直接传递list格式的数据
        :param table: 表名
        :param datas: 列表 [{}, {}, {}]
        :param kwargs:
        :return: 添加行数
        """
        sql, datas = make_batch_sql(table, datas, **kwargs)
        return self.add_batch(sql, datas)

    def update(self, sql) -> Any | None:
        affect_count = None
        conn, cursor = None, None

        try:
            conn, cursor = self.get_connection()
            affect_count = cursor.execute(sql)
            conn.commit()
        except Exception as e:
            log.error(
                """
                error:%s
                sql:  %s
            """
                % (e, sql)
            )
        finally:
            self.close_connection(conn, cursor)

        return affect_count

    def update_smart(self, table, data: Dict, condition) -> int:
        """
        更新
        :param table: 表名
        :param data: 数据 {"xxx":"xxx"}
        :param condition: 更新条件 where后面的条件，如 condition='status=1'
        :return: 影响行数
        """
        sql = make_update_sql(table, data, condition)
        return self.update(sql)

    def delete(self, sql) -> Any | None:
        """
        删除
        :param sql:
        :return: 影响行数
        """
        affect_count = None
        conn, cursor = None, None
        try:
            conn, cursor = self.get_connection()
            affect_count = cursor.execute(sql)
            conn.commit()
        except Exception as e:
            log.error(
                """
                error:%s
                sql:  %s
            """
                % (e, sql)
            )
        finally:
            self.close_connection(conn, cursor)

        return affect_count

    def execute(self, sql) -> Any | None:
        """

        :param sql:
        :return: 影响行数
        """
        affect_count = None
        conn, cursor = None, None
        try:
            conn, cursor = self.get_connection()
            affect_count = cursor.execute(sql)
            conn.commit()
        except Exception as e:
            log.error(
                """
                error:%s
                sql:  %s
            """
                % (e, sql)
            )
        finally:
            self.close_connection(conn, cursor)

        return affect_count

