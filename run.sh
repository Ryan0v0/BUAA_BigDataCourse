hadoop fs -rm -r /user/root/output/outputs/1
hadoop jar /usr/local/hadoop-2.9.2/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar -D mapred.reduce.tasks=5 -mapper "python /root/bigdata/V_hadoop/task1/mapper1.py " -reducer " python /root/bigdata/V_hadoop/task1/reducer1.py " -input /user/root/input/inputs/1.json -output /user/root/output/outputs/1
hadoop fs -getmerge /user/root/output/outputs/1 /root/bigdata/V_hadoop/task1/result.json
