There are three separate queries to test Hadoop performance
There are two subpackages in each query-{1,2,3} directory. They are the mapper and reducer subpackage.

There are separate executables because hadoop-streaming requires an executable to be passed to it which reads from STDIN and writes to STDOUT.

After building the executables, you can run them with hadoop-streaming using something similar to the code snippet below

```
QUERY=1
OUTPUT_NAME="output${QUERY}"
MAPPER_NAME="/home/ec2-user/hadoop_streaming_binaries/query_${QUERY}_mapper"
REDUCER_NAME="/home/ec2-user/hadoop_streaming_binaries/query_${QUERY}_reducer"

bin/hdfs dfs -rm -r ${OUTPUT_NAME}
SECONDS=0
bin/hadoop  jar ./share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
	-input input \
	-output ${OUTPUT_NAME} \
	-mapper ${MAPPER_NAME} \
	-reducer ${REDUCER_NAME}
echo "Query ${QUERY} Processing took $SECONDS seconds"
```
