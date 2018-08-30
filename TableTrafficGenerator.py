#!/usr/bin/python
'''
    @author Jakob Lorberblatt
    @date 2018-08-29
    
'''

# imports
import os,sys,time
from argparse import ArgumentParser
from MySQLdb import connect
from threading import Thread
from md5 import md5 

# constants and defaults
DEFAULT_TABLE_CREATE="create table %s ( id INT AUTO_INCREMENT PRIMARY KEY, vname varchar(64) NOT NULL )"
DEFAULT_DB="test"
DEFAULT_TABLE_NAME="test_table"
DEFAULT_ITERATIONS=1000
DEFAULT_THREAD_COUNT=10
SLEEP_COUNT=2

#classes
class ThreadWrap(Thread):
    '''
    classdocs
    '''


    def __init__(self, connection_wrapper,iteration_count=1000,generator_function=None,sql_text=None):
        '''
        Constructor
        '''
        Thread.__init__(self)
        self.__con = connection_wrapper
        self.__generator = generator_function
        self.__sql_text = sql_text
        self.__count = iteration_count
    
    def run(self):
        if self.__generator:
            for i in range(self.__count):
                sql = self.__generator(i)
                print "%s %s" % (i,sql)
                self.__con.execute(sql)
                
        else:
            for i in range(self.__count):
                self.__con.execute(self.__sql_text)
        self.__con.close()
        self.__con.commit()


class DBConnection:
    def __init__(self,dbcon):
        self.__dbc = dbcon
        self.__curse = dbcon.cursor()
        
    def close(self):
        self.__dbc.close()
        
    def commit(self):
        self.__dbc.commit()
        
    def execute(self,sql):
        self.__curse.execute(sql)
        
    def use(self,db):
        self.__curse.execute('use %s' % db)
        
    def query(self,sql):
        self.__curse.execute(sql)
        return self.__curse.fetchall()
    
    def get_row(self,sql):
        self.__curse.execute(sql)
        result = self.__curse.fetchone()
        if result:
            return result
        
    def get_scalar(self,sql):
        self.__curse.execute(sql)
        result = self.__curse.fetchone()
        if result:
            return result[0]
        

# insert generation function



# main routine entry point
if __name__ == '__main__':
    ap = ArgumentParser()
    ap.add_argument('--user',help="Username to connect to MySQL with")
    ap.add_argument('--password',help="password to use to connect to the mysql instance")
    ap.add_argument('--server',help="server to connect to, default is localhost",default='localhost')
    ap.add_argument('--database',help="database to find the table to load in, default: %s" % DEFAULT_DB,default=DEFAULT_DB)
    ap.add_argument('--table',help="table to load data into, default: %s" % DEFAULT_TABLE_NAME,default=DEFAULT_TABLE_NAME)
    ap.add_argument('--iterations',help="number of rows to add per thread, default: %s" % DEFAULT_ITERATIONS,default=DEFAULT_ITERATIONS)
    ap.add_argument('--threads',help="number of threads operating in parallel, default: %s" % DEFAULT_THREAD_COUNT,default=DEFAULT_THREAD_COUNT)
    values = ap.parse_args()
    
    def generate_insert(iVal):
        m = md5()
        m.update(str(iVal))
        return "insert into %s ( vname ) VALUES ( '%s')" % (values.table,m.hexdigest())
    
    def get_connection():
        return DBConnection(connect(user=values.user,passwd=values.password,host=values.server,db=values.database))
    
    
    THREADS=[]
    for i in range(values.threads):
        thr = ThreadWrap(get_connection(),values.iterations,generate_insert)
        thr.setDaemon(True)
        thr.start()
        THREADS.append(thr)
        
    while 1:
        for t in THREADS:
            try:
                if not t.isAlive():
                    THREADS.remove(t)
            except:
                pass
        if not THREADS:
            break
        else:
            try:
                time.sleep(SLEEP_COUNT)
            except:
                pass
            
    print "Completed Execution at %s" % time.ctime(time.time())
    