#!/usr/bin python
#-*- coding:utf-8 -*-
import  logging
import  threading
import functools

class Dict(dict):
    def __init__(self,name = (),values = (), **kw):
        super(Dict,self).__init__(**kw)
        for k,v in zip(name,values):
            self[k] = v
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s' " %(key))
    def __setattr__(self, key, value):
        self[key] = value



class DBError(Exception):
    pass

class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    def cleanup(self):
        if self.connection:
            connection = self.connection
            logging.info('close then connection <%s> ...' %hex(id(connection)))
            connection.close()
            self.connection = None
    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open the connection <%s> ...' %hex(id(connection)))
            self.connection = connection
        return  self.connection.cursor()


class _Engine(object):
    def __init__(self,connect):
        self._connect = connect

    def connect(self):
        return self._connect()

engine = None
def create_engine(user,password,database,host='127.0.0.1',port=3306,**kw):
    import  mysql.connector
    global  engine
    if engine is not None:
        raise  DBError("Engine is already init")
    parms = dict(user = user,password = password,database = database,host = host,port = port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k,v in defaults.iteritems():
        parms[k] = kw.pop(k,v)

    parms.update(kw)
    parms['buffered'] = True
    engine = _Engine(lambda :mysql.connector.connect(**parms))
    logging.info('Init Mysql engine <%s> ok' %hex(id(engine)))


class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return  not self.connection is None

    def init(self):
        logging.info("open the lazy connection")
        self.connection = _LasyConnection()

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        self.connection.cursor()

_db_ctx = _DbCtx()


###定义可以使用with上下文对象
class _ConnectionCtx(object):
    def __enter__(self):
        global  _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            print "hello __enter__"
            self.should_cleanup = True
        return  self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    return  _ConnectionCtx()

###with装饰器
def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args,**kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return  _wrapper

class  _TransactionCtx(object):
    def __enter__(self):
        global  _db_ctx
        self.should_close_cnn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_cnn = True

        _db_ctx.transactions = _db_ctx.transactions + 1
        logging.info("begin the transation " if _db_ctx.transactions == 1 else 'join current tarnsaction ...')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions == 0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()

        finally:
            if self.should_close_cnn:
                _db_ctx.cleanup()



    def commit(self):
        global _db_ctx
        logging.info('commit transation ...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok')
        except:
            logging.warning('commit failed  try rollback ...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok')
            raise


    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction ... ')
        _db_ctx.connection.rollback()
        logging.warning('rollback ok')

def  transaction():
    return  _TransactionCtx()

def with_transaction(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _TransactionCtx():
            return func(*args,**kw)
    return _wrapper

def _select(sql,first,*args):
    global  _db_ctx
    cursor = None
    sql = sql.replace('?','%s')
    logging.info('sql :%s args :%s' %(sql,args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.foreachone()
            if not values:
                return  None
            return Dict(names,values)
        return  [Dict(names,x) for x in cursor.foreachall]
    finally:
        if cursor:
           cursor.close()

@with_connection
def select_one(sql,*args):
    return  _select(sql,True,*args)

@with_connection
def select_int(sql,*args):
    '''
    Execute select SQL and expected one int and only one int result.
    :param sql:
    :param args:
    :return:
    '''

    d = _select(sql,True,*args)
    if len(d) != 1:
        raise MultiColumnsError('Expect only one column.')
    return d.values()[0]

@with_connection
def select(sql,*args):
    '''
    exeucte sql
    :param sql:
    :param args:
    :return:
    '''
    return  _select(sql,False,*args)

@with_connection
def _update(sql,*args):
    global  _db_ctx
    cursor = None
    sql = sql.replace('?','%s')
    logging.info('SQL:%s ARGS:%s' %(sql,args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return  r
    finally:
        if cursor:
            cursor.close()

def insert(table,**kw):
    clos,args = zip(*kw.iteritems())
    sql='insert into `%s` (%s) values (%s)' %(table,','.join(['`%s`' %clo for clo in clos]),','.join(['?' for i in range(len(clos))]))
    return _update(sql,*args)

def update(sql,*args):
    return _update(sql,*args)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('py_user', 'python123456', 'python_db','192.168.31.32')
    update('drop table if exists user')
    update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest

    doctest.testmod
