# -*- coding=utf-8 -*-
import paramiko
import conf
import logging
import time
import datetime
from mysql_op import MySQL


def ssh_cmd(ip,port,cmd,user,passwd):
    str = u"ssh %s@%s '%s'" % (user, ip, cmd)
    result = ""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip,port,user,passwd,timeout=5)
        stdio,stdout,stderr = ssh.exec_command(cmd.encode('utf-8'))
        result = stdout.read()
        ssh.close()
        return 0,result
    except Exception , e:
        return -1,'ssh_cmd error'


def get_mysql(db_info):
    mysql = MySQL(db_info["ip"], db_info["user"], db_info["passwd"], db_info["port"])
    print db_info
    mysql.selectDb(db_info["name"])

    return mysql

#获取数据库对应的服务器列表
def get_svrlist( tag, iptype, flag):
    mysql = get_mysql(conf.server_db_info)
    sql_statement = 'select %s, user, passwd, onlyflag, tag from %s;' % (iptype, conf.SERVERLIST_TBNAME)
    mysql.query(sql_statement)
    sqltuple = mysql.fetchAllTuple()

    svrlist = []
    for ip, user, passwd, onlyflag, sqltag in sqltuple:
        if tag not in sqltag:
            continue
        if flag not in sqltag: #gs,dir...
            continue
        try:
            pt = int(onlyflag.split("_")[1])
            zone = int(onlyflag.split("_")[2])
        except:
            continue

        logging.error("%s,%s,%s,%d,%d,%s" % (ip,user,passwd,pt,zone,sqltag))
        svrlist.append({'ip':ip, 'user':user, 'passwd':passwd, 'pt':pt, 'zone':zone, 'tag': sqltag.split('|')[-1]})
    return svrlist

def get_date(day_delta = 0):
    date = time.strftime("%Y%m%d", time.localtime(time.time()))

    if day_delta > 0:
        day_delta = 0 - day_delta
        predate_time = datetime.datetime.strptime(date, "%Y%m%d") + datetime.timedelta(days = day_delta)
        date = predate_time.strftime("%Y%m%d")

    return date