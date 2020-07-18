#!/bin/bash
WK_DIR=$(cd `dirname $0`;cd "../";pwd)
echo $WK_DIR
ps -ef |grep ${WK_DIR}/tool/main_new2.py |awk '{print $2}'|xargs kill -9
