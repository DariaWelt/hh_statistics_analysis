#!/bin/bash


python3 $SPARK_APP
# it's crutch to prevent container stoping, remove it if you need
echo "-------------HANDLER ENDED-------------"
tail -f /dev/null