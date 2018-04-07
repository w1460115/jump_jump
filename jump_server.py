
import conf
import logging
import time
import datetime
import os
from mysql_op import MySQL

def get_mysql(db_info):
    mysql = MySQL("ip", "username", "passwd", "port")
    print db_info
    mysql.selectDb('opsys')
    print("get mysql")
    return mysql



def get_svrlist( ):
    mysql = get_mysql(conf.server_db_info)
    # sql_statement = 'select %s, user, passwd, onlyflag, tag from %s;' % (iptype, conf.SERVERLIST_TBNAME)
    sql_statement= 'select * from jump_server_list'
    mysql.query(sql_statement)
    sqltuple = mysql.fetchAllTuple()


    return sqltuple

def serverlist_view():
    choice_server={}
    serverlist=get_svrlist()
    print("please choice server")
    fmt="%4s%20s%15s%15s"
    for viewer in serverlist:
        print fmt%(str(int(viewer[0])),viewer[1],viewer[2],viewer[3])
    id = input("enter id")
    for serverinfo in serverlist:
        if id == int (serverinfo[0]):
            choice_server=serverinfo
            break
    if choice_server:
        return choice_server
    else :
        print(" please retry  ")


def ssh_login(server_info):
    print(server_info)
    print("login %s" %(server_info[2]))
    if server_info[1]==server_info[4]:
        cmd="sshpass -p \"%s\" ssh %s@%s" %(server_info[6],server_info[5],server_info[1])
        print("try login ")

        # os.system(cmd)
    else :
        cmd="passwd=\'%s\';sshpass -p $passwd ssh -t %s@%s \"sshpass -p $passwd ssh %s@%s \"" %(server_info[6],server_info[5],server_info[4],server_info[5],server_info[1])
    os.system(cmd)
if __name__ == '__main__':
    ssh_login(serverlist_view())