#!/usr/bin/python
#-*-coding:utf-8

import sys
import os
import paramiko
import time
import datetime
#sys.path.append(r'/root')
#from check_mongo_stat.py import check

mongod_host = ['189.1.1.72','189.1.1.24','189.1.1.20']
mongos_host = ['189.1.1.72']
root_pswd="123456"
mongo_work_home="/smart/mongodb"
mongos_port="27050"
config_port="27030"
shd_mst_port="27010"
shd_slv_port="27011"
shd_abt_port="27012"
config_repl_nm="config"
repl_list = {"shard_repl_0":"rep1","shard_repl_1":"rep2","shard_repl_2":"rep3"}

def mk_dir_for_mongo():
    for h in mongod_host:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(h , port=22 , username='root' , password=root_pswd)
        exec_cmd = ("[ -d %s ] || mkdir -p %s;" %(mongo_work_home , mongo_work_home))
        exec_cmd = exec_cmd + ("cd %s;mkdir -p mongos/log;mkdir -p mongos/conf;mkdir -p mongos/data;" %(mongo_work_home))
        exec_cmd = exec_cmd + ("cd %s;mkdir -p config/log;mkdir -p config/conf;mkdir -p config/data;" %(mongo_work_home))
        exec_cmd = exec_cmd + ("cd %s;mkdir -p master/log;mkdir -p master/conf;mkdir -p master/data;" %(mongo_work_home))
        exec_cmd = exec_cmd + ("cd %s;mkdir -p slave/log;mkdir -p slave/conf;mkdir -p slave/data;" %(mongo_work_home))
        exec_cmd = exec_cmd + ("cd %s;mkdir -p arbiter/log;mkdir -p arbiter/conf;mkdir -p arbiter/data;" %(mongo_work_home))
        print(exec_cmd)
        stdin,stdout,stderr = ssh.exec_command(exec_cmd)
        channel = stdout.channel
        status = channel.recv_exit_status()
        print(status)
        if status != 0:
            err_info = stderr.readlines(1)
            print("%s make mongodb work directory failed , err info:%s!" %(h,err_info))
            exit(1)

def clean_up_mongo():
    for h in mongod_host:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(h , port=22 , username='root' , password=root_pswd)
        exec_cmd = ('killall -9 mongod;killall -9 mongos')
        print(exec_cmd)
        stdin,stdout,stderr = ssh.exec_command(exec_cmd)
        exec_cmd = ('cd /smart/mongodb;find ./ -mindepth 3 -name "*" | xargs rm -rf')
        print(exec_cmd)
        stdin,stdout,stderr = ssh.exec_command(exec_cmd)
        channel = stdout.channel
        status = channel.recv_exit_status()
        print(status)
        if status != 0:
            err_info = stderr.readlines(1)
            print("%s err info:%s!" %(h,err_info))
            exit(1)

def dep_mongod_config():
    n = 0
    for h in mongod_host:
        i = n
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(h , port=22 , username='root' , password=root_pswd)
        exec_cmd = ('killall -9 mongod')
        print(exec_cmd)
        stdin,stdout,stderr = ssh.exec_command(exec_cmd)
        exec_cmd = ('cd /smart/mongodb;find ./ -mindepth 3 -name "*" | xargs rm -rf')
        print(exec_cmd)
        stdin,stdout,stderr = ssh.exec_command(exec_cmd)
        channel = stdout.channel
        status = channel.recv_exit_status()
        print(status)
        if status != 0:
            err_info = stderr.readlines(1)
            print("%s err info:%s!" %(h,err_info))
            exit(1)

        i = (0 if (i>=3) else i)
        repl_nm = ("shard_repl_%d" %(i))
        print(repl_nm)

        exec_cmd_list = []
        exec_cmd_list.append("mongod --configsvr --port %s --logpath /smart/mongodb/config/log/config.log --dbpath /smart/mongodb/config/data --config /etc/mongod.conf --pidfilepath /smart/mongodb/config/config.pid --replSet %s/%s:%s" %(config_port , config_repl_nm , h , config_port))
        exec_cmd_list.append("mongod --shardsvr --port %s --logpath /smart/mongodb/master/log/mongod.log --dbpath /smart/mongodb/master/data --config /etc/mongod.conf --pidfilepath /smart/mongodb/master/master.pid --replSet %s/%s:%s" %(shd_mst_port , repl_list[repl_nm] , h , shd_mst_port))
        i = i + 1
        i = (0 if (i>=3) else i)
        repl_nm = ("shard_repl_%d" %(i))
        print(repl_nm)
        exec_cmd_list.append("mongod --shardsvr --port %s --logpath /smart/mongodb/slave/log/mongod.log --dbpath /smart/mongodb/slave/data --config /etc/mongod.conf --pidfilepath /smart/mongodb/slave/slave.pid --replSet %s/%s:%s" %(shd_slv_port , repl_list[repl_nm] , mongod_host[i] , shd_slv_port))
        i = i + 1
        i = (0 if (i>=3) else i)
        repl_nm = ("shard_repl_%d" %(i))
        print(repl_nm)
        exec_cmd_list.append("mongod --shardsvr --port %s --logpath /smart/mongodb/arbiter/log/mongod.log --dbpath /smart/mongodb/arbiter/data --config /etc/mongod.conf --pidfilepath /smart/mongodb/arbiter/arbiter.pid --replSet %s/%s:%s" %(shd_abt_port , repl_list[repl_nm] , mongod_host[i] , shd_abt_port))
        if isinstance(exec_cmd_list, list):
            for l in exec_cmd_list:
                print(l)
                stdin,stdout,stderr = ssh.exec_command(l)
                channel = stdout.channel
                status = channel.recv_exit_status()
                print(status)
                if status != 0:
                    err_info = stderr.readlines(1)
                    print("%s start config and shardsvr process failed , err info:%s!" %(h,err_info))
                    exit(1)
        n = n + 1

def init_config_mongod():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(mongod_host[0] , port=22 , username='root' , password=root_pswd)

    exec_cmd_list = []
    exec_cmd_list.append("mongo --port %s --quiet --eval \"config={_id:'%s',configsvr:true,members:[{_id:0,host:'%s:%s',priority:4},{_id:1,host:'%s:%s', priority:3},{_id:2,host:'%s:%s', priority:2}]};rs.initiate(config)\"" %(config_port , config_repl_nm , mongod_host[0] , config_port , mongod_host[1] , config_port , mongod_host[2] , config_port))
    if isinstance(exec_cmd_list, list):
        for l in exec_cmd_list:
            print(l)
            stdin,stdout,stderr = ssh.exec_command(l)
            channel = stdout.channel
            status = channel.recv_exit_status()
            print(status)
            if status != 0:
                err_info = stderr.readlines(1)
                print("%s init configsvr process failed , err info:%s!" %(mongod_host[0],err_info))
                exit(1)

def init_shard_mongod():
    n = 0
    for h in mongod_host:
        m = n
        s = m + 1
        a = s + 1

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(h , port=22 , username='root' , password=root_pswd)

        s = m + 1
        s = (0 if (s>=3) else s)
        a = s + 1
        a = (0 if (a>=3) else a)
        repl_nm = ("shard_repl_%d" %(m))

        exec_cmd_list = []
        exec_cmd_list.append("mongo --port %s --quiet --eval \"config={_id:'%s',members:[{_id:0,host:'%s:%s',priority:4},{_id:1,host:'%s:%s', priority:3},{_id:2,host:'%s:%s',arbiterOnly:true}]};rs.initiate(config)\"" %(shd_mst_port , repl_list[repl_nm] , h , shd_mst_port , mongod_host[a] , shd_slv_port , mongod_host[s] , shd_abt_port))
        if isinstance(exec_cmd_list, list):
            for l in exec_cmd_list:
                print(l)
                stdin,stdout,stderr = ssh.exec_command(l)
                channel = stdout.channel
                status = channel.recv_exit_status()
                print(status)
                if status != 0:
                    err_info = stderr.readlines(1)
                    print("%s init shardsvr process failed , err info:%s!" %(h,err_info))
                    exit(1)

        n = n + 1

def dep_mongos():
    for h in mongod_host:
        m = 0
        s = m + 1
        a = s + 1

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(h , port=22 , username='root' , password=root_pswd)

        exec_cmd_list = []
        exec_cmd_list.append("mongos --configdb %s/%s:%s,%s:%s,%s:%s --port %s --fork --logpath /smart/mongodb/mongos/mongos.log" %(config_repl_nm , mongod_host[m] , config_port , mongod_host[s] , config_port , mongod_host[a] , config_port , mongos_port))
        if isinstance(exec_cmd_list, list):
            for l in exec_cmd_list:
                print(l)
                stdin,stdout,stderr = ssh.exec_command(l)
                channel = stdout.channel
                status = channel.recv_exit_status()
                print(status)
                if status != 0:
                    err_info = stderr.readlines(1)
                    print("%s start mongos process failed , err info:%s!" %(h,err_info))
                    exit(1)

# only add shards into mongos
def init_mongos():
    m = 0
    s = m + 1
    a = s + 1

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(mongod_host[m] , port=22 , username='root' , password=root_pswd)

    repl_nm_1 = ("shard_repl_%d" %(m))
    repl_nm_2 = ("shard_repl_%d" %(s))
    repl_nm_3 = ("shard_repl_%d" %(a))

    exec_cmd_list = []
    exec_cmd_list.append("mongo --port %s --quiet --eval \"sh.addShard(\'%s/%s:%s,%s:%s,%s:%s\');sh.addShard(\'%s/%s:%s,%s:%s,%s:%s\');sh.addShard(\'%s/%s:%s,%s:%s,%s:%s\');\"" %(mongos_port , repl_list[repl_nm_1] , mongod_host[m] , shd_mst_port , mongod_host[a] , shd_slv_port , mongod_host[s] , shd_abt_port , repl_list[repl_nm_2] , mongod_host[s] , shd_mst_port , mongod_host[m] , shd_slv_port , mongod_host[a] , shd_abt_port , repl_list[repl_nm_3] , mongod_host[a] , shd_mst_port , mongod_host[s] , shd_slv_port , mongod_host[m] , shd_abt_port))
    if isinstance(exec_cmd_list, list):
        for l in exec_cmd_list:
            print(l)
            stdin,stdout,stderr = ssh.exec_command(l)
            channel = stdout.channel
            status = channel.recv_exit_status()
            print(status)
            if status != 0:
                err_info = stderr.readlines(1)
                print("%s add shard into mongos failed , err info:%s!" %(mongod_host[m],err_info))
                exit(1)

mk_dir_for_mongo()
clean_up_mongo()
dep_mongod_config()
time.sleep(1)
init_config_mongod()
time.sleep(1)
init_shard_mongod()
time.sleep(1)
dep_mongos()
init_mongos()

