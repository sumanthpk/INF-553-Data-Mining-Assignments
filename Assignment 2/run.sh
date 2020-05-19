#!/bin/bash
export PYSPARK_PYTHON=python3.6


########## Task 1 ##########

if [ -f "task1.py" ]
then
    start1=$(date +%s%3N)
    spark-submit task1.py  1 4 $ASNLIB/publicdata/small2.csv task1_1_ans
    end1=$(date +%s%3N)

    start2=$(date +%s%3N)
    spark-submit task1.py  2 9 $ASNLIB/publicdata/small2.csv task1_2_ans
    end2=$(date +%s%3N)
else
    echo "task1.py not found"
fi
