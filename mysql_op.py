#!/usr/bin/python
#coding=utf-8

import MySQLdb
import copy
OperationalError = MySQLdb.OperationalError

class MySQL:
    def __init__(self, host, user, password, port=3306,charset="utf8"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.charset = charset
        try:
            self.conn=MySQLdb.connect(host=self.host, port=self.port,user=self.user,passwd=self.password)
            self.conn.autocommit(False)
            self.conn.set_character_set(self.charset)
            self.cur = self.conn.cursor()
        except MySQLdb.Error as e:
            print("Mysql Error %d:%s" % ( e.args[0], e.args[1]))

    def __del__(self):
        self.close()

    def selectDb(self,db):
        try:
            self.conn.select_db(db)
        except MySQLdb.Error as e:
            print("Mysql Error %s" % e)
    def query(self,sql):
        try:
            n=self.cur.execute(sql)
            return n
        except MySQLdb.Error as e:
            print("Mysql Error:%s\nSQL:%s" %(e,sql))
    def dropTable(self, table_name):
        _prefix="".join(['DROP TABLE IF EXISTS `',table_name,'`;'])
        _sql = _prefix;
        print _sql
        return self.cur.execute(_sql)

    def createTable(self,table_name, data, primary_keys, data_size):
        columns=data.keys()
        if not columns:
            return None
   
        keys = ",".join(["`%s`" % key for key in primary_keys])
        _prefix="".join(['CREATE TABLE IF NOT EXISTS`',table_name,'` ('])
        _fields=",".join(["".join(['`',column,'`', ' varchar(',str(data_size.get(column, 30)), ')' ]) for column in columns])
        _sql="".join([_prefix,_fields,", PRIMARY KEY (",keys,")) ENGINE=MyISAM DEFAULT CHARSET=utf8"])
        return self.cur.execute(_sql)

    def fetchRow(self):
        result = self.cur.fetchone()
        return result
                          
    def fetchAllTuple(self):
        tuple_data = ()
        try:
            tuple_data = self.cur.fetchall()
        except MySQLdb.Error as e:
            print("Mysql Error: %s" % e) 
            tuple_data = ()

        return tuple_data

    def fetchAll(self):
        result=self.cur.fetchall()
        desc =self.cur.description
        
        d = []
        for inv in result:
            _d = {}
            for i in range(0,len(inv)):
                _d[desc[i][0]] = str(inv[i])
                d.append(_d)
        return d
                          
    def is_column_exist(self, table_name, column):
        _sql = 'select count(*) from information_schema.columns where table_name=\"%s\" and column_name=\"%s\"' % (table_name, column)
        self.query(_sql)
        result = self.fetchRow()
        return result

    def add_column(self, table_name, column, size):
        _sql = 'alter table %s add %s varchar(%d)' % (table_name, column, size)
        self.query(_sql)
        return self.commit()

    def add_primary(self, table_name, column, primary_keys):
        _sql = 'alter table %s drop primary key' % table_name
        self.query(_sql)
        _params = ",".join(["%s" % key for key in primary_keys])
        _sql = 'alter table %s add primary key(%s)' % (table_name, _params)
        self.query(_sql)
        return self.commit()

    def is_need_drop_table(self, table_name, data):
        columns = data.keys()
        for column in columns:
            result = self.is_column_exist(table_name, column)
            for count in result:
                print count
                if int(count) <= 0:
                    print "column %s is not exist" % column
                    return True  
        return False       
 
    def insert(self,table_name,data):
        columns=data.keys()
        _prefix="".join(['INSERT INTO `',table_name,'`'])
        _fields=",".join(["".join(['`',column,'`']) for column in columns])
        _values=",".join(["%s" for i in range(len(columns))])
        _sql="".join([_prefix,"(",_fields,") VALUES (",_values,")"])
        _params=[data[key] for key in columns]
        return self.cur.execute(_sql,tuple(_params))

    def insert_ignor(self,table_name,data):
        columns=data.keys()
        _prefix="".join(['INSERT IGNORE INTO `',table_name,'`'])
        _fields=",".join(["".join(['`',column,'`']) for column in columns])
        _values=",".join(["%s" for i in range(len(columns))])
        _sql="".join([_prefix,"(",_fields,") VALUES (",_values,")"])
        _params=[data[key] for key in columns]
        return self.cur.execute(_sql,tuple(_params))

    def insert_exist_update(self, table_name, data):
        columns=data.keys()
        _prefix="".join(['INSERT INTO `',table_name,'`'])
        _fields=",".join(["".join(['`',column,'`']) for column in columns])
        _values=",".join(["%s" for i in range(len(columns))])
        _sql="".join([_prefix,"(",_fields,") VALUES (",_values,")"])
        _sql+=" ON DUPLICATE KEY UPDATE "
        
        index = 0
        for column in columns:
            if index == 0:
                _sql += "`%s`" % (column) + "=%s"
            else:
                _sql += ",`%s`" % (column) + "=%s"
            index = index + 1
        _params=[data[key] for key in columns]
        _params_copy = copy.deepcopy(_params)
        for v in _params_copy:
            _params.append(v)
        return self.cur.execute(_sql,tuple(_params))
    
    def insert_exist_update_onebyone(self, table_name, primate_keys, data_list):
        _params_list = []
        for data in data_list:
            columns=data.keys() 
            other_keys_index = []
            for i,column in enumerate(columns):
                if column not in primate_keys:
                    other_keys_index.append(i) 

            _prefix="".join(['INSERT INTO `',table_name,'` '])
            _fields=",".join(["".join(['`',column,'`']) for column in columns])
            _values=",".join(["%s" for i in range(len(columns))])
            _sql="".join([_prefix,"(",_fields,") VALUES (",_values,")"])
            _sql+=" ON DUPLICATE KEY UPDATE "
            
            index = 0
            for column in columns:
                if column in primate_keys:
                    continue
                if index == 0:
                    _sql += "`%s`" % (column) + "=VALUES(%s)" % (column)
                else:
                    _sql += ",`%s`" % (column) + "=VALUES(%s)" % (column)
                index = index + 1

            _params=[data[key] for key in columns]
            ret = self.cur.execute(_sql,tuple(_params))

        return ret 
    
    def insert_exist_update_many(self, table_name, primary_keys, data_list):
        _params_list = []
        for data in data_list:
            columns=data.keys()
            other_keys_index = []
            for i,column in enumerate(columns):
                if column not in primary_keys:
                    other_keys_index.append(i) 

            _prefix="".join(['INSERT INTO `',table_name,'` '])
            _fields=",".join(["".join(['`',column,'`']) for column in columns])
            _values=",".join(["%s" for i in range(len(columns))])
            _sql="".join([_prefix,"(",_fields,") VALUES (",_values,")"])
            _sql+=" ON DUPLICATE KEY UPDATE "
            
            index = 0
            for column in columns:
                if column in primary_keys:
                    continue
                if index == 0:
                    _sql += "`%s`" % (column) + "=VALUES(%s)" % (column)
                else:
                    _sql += ",`%s`" % (column) + "=VALUES(%s)" % (column)
                index = index + 1

            _params=[data[key] for key in columns]
            _params_list.append(tuple(_params))

        print _sql
        return self.cur.executemany(_sql,_params_list)
                        
    def update(self,tbname,data,condition):
        _fields=[]
        _prefix="".join(['UPDATE `',tbname,'`','SET'])
        for key in data.keys():
            _fields.append("%s = %s" % (key,data[key]))
        _sql="".join([_prefix ,_fields, " WHERE ", condition ])
                            
        return self.cur.execute(_sql)
                        
    def delete(self,tbname,condition):
        _prefix="".join(['DELETE FROM  `',tbname,'`',' WHERE '])
        _sql="".join([_prefix,condition]) 
        print _sql
        return self.cur.execute(_sql)
    
    def delete_by_item(self,tbname,data):
        columns=data.keys()
        _prefix="".join(['DELETE FROM  `',tbname,'`',' WHERE '])
        _sql=_prefix
        index = 0
        for column in columns:
            if index == 0:
                _sql += "`%s`" % (column) + "=%s"
            else:
                _sql += " and `%s`" % (column) + "=%s"
            index = index + 1
        _params=[data[key] for key in columns]
        print _sql,_params
        return self.cur.execute(_sql,tuple(_params))
                          
    def getLastInsertId(self):
        return self.cur.lastrowid
                          
    def rowcount(self):
        return self.cur.rowcount
                          
    def commit(self):
        self.conn.commit()
                        
    def rollback(self):
        self.conn.rollback()
                          
    def close(self):
        self.cur.close()
        self.conn.close()

        #create table map (id int,x int, y int);

if __name__=='__main11__':
    n=MySQL('127.0.0.1','root','renlong123',3306)
    n.selectDb('test')
    sqlstr = "create table maps (id int, xx int, yy int);"
    n.query(sqlstr)
    n.commit()
    '''
    tbname='map'
    a=({'id':3,'x':3,'y':3},{'id':4,'x':4,'y':4},{'id':5,'x':5,'y':5})
    for d in a:
        n.insert(tbname,d)
    n.commit()
    '''
