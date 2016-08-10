
This is the Java MapReduce example taken from 'Hadoop The Definitive Guide 4th Edition' - Part 1 - Chapter 2

The example uses Mapreduce framework to find the highest temperatures recorded per year from NCDC data files.

An example of the same Map/Reduce logic using unix command line tools is in the MapReduce_UnixTools directory (see the README file in that directory for details).

To run the Hadoop Java MapReduce example: -

1) Copy the data/ncdc directory into HDFS: -

		hdfs dfs -copyFromLocal ncdc /user/martinr/.

2) Add the jar to HADOOP_CLASSPATH

		export HADOOP_CLASSPATH=mr_tempdata.jar

3) Run the hadoop job:-

		hadoop com.gmail.martinprobson.hadoop.MaxTemperature /user/martinr/ncdc /user/martinr/ncdc/output

4) Check the output : -

		hdfs dfs -cat /user/martinr/ncdc/output/*

5) Output should be the same result as with Unix tools: -

1951    278
1952    313
1953    324
1954    300
1955    296
1956    278
1957    307
1958    289
1959    307
1960    300
1961    302
1962    246
1963    428
		
