#!/bin/bash
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -input /user/hadoop/docgen/input -output /user/hadoop/docgen/output -file /home/hadoop/rs_mapper.py -mapper /home/hadoop/rs_mapper.py -file /home/hadoop/rs_reducer_var.py -reducer /home/hadoop/rs_reducer_var.py -cmdenv K=500 -numReduceTasks 10
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -input /user/hadoop/docgen/output -output /user/hadoop/wc/output -file /home/hadoop/wc_mapper.py -mapper /home/hadoop/wc_mapper.py -file /home/hadoop/wc_reducer.py -reducer /home/hadoop/wc_reducer.py 
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -input /user/hadoop/wc/output -output /user/hadoop/counts/output -file /home/hadoop/total_mapper.py -mapper /home/hadoop/total_mapper.py -file /home/hadoop/total_reducer.py -reducer /home/hadoop/total_reducer.py -numReduceTasks 1
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -input /user/hadoop/wc/output -output /user/hadoop/IDF/output -file /home/hadoop/IDF_mapper.py -mapper /home/hadoop/IDF_mapper.py -file /home/hadoop/IDF_reducer.py -reducer /home/hadoop/IDF_reducer.py -cacheFile /user/hadoop/counts/output/part-00000#part-00000
