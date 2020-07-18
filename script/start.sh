#!/bin/bash
WK_DIR=$(cd `dirname $0`;cd "../";pwd)

nohup python ${WK_DIR}/tool/main_new2.py >/dev/null 2>&1 &
