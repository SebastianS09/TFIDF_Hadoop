#!/bin/bash

hadoop fs -rm -R /user/hadoop/*

hdfs dfs -mkdir /user/hadoop/docgen
hdfs dfs -mkdir /user/hadoop/docgen/input 
hdfs dfs -put 5000-8.txt /user/hadoop/docgen/input
