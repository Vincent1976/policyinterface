import pymysql
from DBUtils.PooledDB import PooledDB
import config

POOL = PooledDB(
    creator = pymysql,  # 使用链接数据库的模块
    maxconnections = 6,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached = 2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached = 5,  # 链接池中最多闲置的链接，0和None不限制
    maxshared = 3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking = True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage = None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession = [],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
    ping = 0,
    # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
    host = config.HOST,
    port = config.PORT,
    password = config.PASSWORD,
    user = config.USERNAME,
    database = config.DATABASE,
    charset = 'utf8'
)


class SQLHelper(object):

    def create_conn_cursor(self):
        # 创建连接
        conn = POOL.connection()
        # 创建游标
        # cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        # 返回conn, cursor
        return conn, cursor

    @staticmethod
    def fetch_one(sql, args):
        conn = POOL.connection()  # 通过连接池链接数据库
        cursor = conn.cursor()   # 创建游标
        cursor.execute(sql, args)  # 执行sql语句
        result = cursor.fetchone()  # 取的sql查询结果
        conn.close()  # 关闭链接
        return result

    @staticmethod
    def fetch_many(sql, args):
        conn = POOL.connection()
        cursor = conn.cursor()
        cursor.execute(sql, args)
        result = cursor.fetchmany()
        conn.close()
        return result

    @staticmethod
    def fetch_all(sql, args):
        conn = POOL.connection()
        cursor = conn.cursor()
        cursor.execute(sql, args)
        result = cursor.fetchall()
        conn.close()
        return result

    @staticmethod
    def insert_one(sql, args):
        conn = POOL.connection()  # 通过连接池链接数据库
        cursor = conn.cursor()   # 创建游标
        res = cursor.execute(sql, args)
        conn.commit()
        print(res)
        conn.close()
        return res

    @staticmethod
    def insert_many(sql, args):
        conn = POOL.connection()  # 通过连接池链接数据库
        cursor = conn.cursor()   # 创建游标
        res = cursor.executemany(sql, args)
        conn.commit()
        print(res)
        conn.close()
        return res

    @staticmethod
    def update(sql, args):
        conn = POOL.connection()  # 通过连接池链接数据库
        cursor = conn.cursor()   # 创建游标
        res = cursor.execute(sql, args)
        conn.commit()
        print(res)
        conn.close()
        return res

    @staticmethod
    def delete(self, sql, args):
        conn = POOL.connection()  # 通过连接池链接数据库
        cursor = conn.cursor()   # 创建游标
        res = cursor.execute(sql, args)
        conn.commit()
        print(res)
        conn.close()
        return res
