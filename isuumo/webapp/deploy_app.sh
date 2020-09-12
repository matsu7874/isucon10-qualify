#!/bin/bash
set -eux

scp ./python/app.py isucon-server-1:/home/isucon/isuumo/webapp/python/
scp ./mysql/db/init.sh  isucon-server-1:/home/isucon/isuumo/webapp/mysql/db/
scp ./mysql/db/0_Schema.sql isucon-server-1:/home/isucon/isuumo/webapp/mysql/db/
scp ./mysql/db/1_DummyEstateData.sql isucon-server-1:/home/isucon/isuumo/webapp/mysql/db/
scp ./mysql/db/2_DummyChairData.sql isucon-server-1:/home/isucon/isuumo/webapp/mysql/db/
scp ./mysql/db/3_Create_search_estate.sql isucon-server-1:/home/isucon/isuumo/webapp/mysql/db/
scp ./mysql/db/3_Create_search_chair.sql isucon-server-1:/home/isucon/isuumo/webapp/mysql/db/
ssh isucon-server-1 "/home/isucon/isuumo/webapp/mysql/db/init.sh"
ssh isucon-server-1 "sudo systemctl restart isuumo.python.service"
