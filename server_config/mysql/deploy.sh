#!/bin/bash
set -eux

scp mysqld.cnf isucon-server-1:/home/isucon/work/
ssh isucon-server-1 "sudo cp /home/isucon/work/mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf"
ssh isucon-server-1 "sudo systemctl restart mysql.service"
