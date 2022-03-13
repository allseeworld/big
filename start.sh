#!/bin/bash
bash hadoop jar /usr/lib/hadoop-current/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -mapper "mymap.py" \
    -file /home/student3/lijiangyong/big/mymap.py \
    -file /home/student3/lijiangyong/big/myreduce.py \
    -reducer "myreduce.py" \
    -input "/user/lijiangyong/data/HTTP_20130313143750.dat" \
    -output "/home/student3/lijiangyong/output"