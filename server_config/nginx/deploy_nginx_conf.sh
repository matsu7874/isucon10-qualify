#!/bin/bash
set -eux

scp isuumo.conf isucon-server-1:/home/isucon/work/
ssh isucon-server-1 "sudo cp /home/isucon/work/nginx.conf /etc/nginx/nginx.conf"
ssh isucon-server-1 "sudo cp /home/isucon/work/isuumo.conf /etc/nginx/sites-available/isuumo.conf"
ssh isucon-server-1 "sudo nginx -t"
ssh isucon-server-1 "sudo systemctl restart nginx.service"
