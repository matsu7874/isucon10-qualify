scp mysqld.cnf isucon-server-1:/home/isucon/work/
ssh isucon-server-1 "sudo mv /home/isucon/work/mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf"
ssh isucon-server-1 "sudo systemctl restart mysql.service"
