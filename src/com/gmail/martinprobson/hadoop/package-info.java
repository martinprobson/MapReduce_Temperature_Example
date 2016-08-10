/**
 * 
 */
/**
 * @author martinr
 * 
 * To run these on a Hadoop sample cluster: -
 * 
 * 1) Start HDFS and YARN daemons: -
 * 		start-dfs.sh && start-yarn.sh
 * 
 * 		goto http://localhost:8088/cluster to see Hadoop dashboard
 * 		and http://localhost:50070/dfshealth.html#tab-overview for the namenode.start
 * 
 * 2) Put the input files into HDFS via: -
 * 
 * 		hdfs dfs -put 19xx /user/martinr/ncdc/.
 *
 * 3) Run as follows: -
 * 
 * 		export HADOOP_CLASSPATH=mr_tempdata.jar
 * 		hadoop com.gmail.martinprobson.hadoop.MaxTemperature /user/martinr/ncdc2 /user/martinr/output3
 * 
 * 
 * TODO Finish documentation above
 * TODO Get this project into git
 * TODO Work out why code fails when directory specified instead of single file
 * TODO WOrk out if gzip files can be used as input to the mapper
 * TODO Run this in a local Hadoop Instance.
 */
package com.gmail.martinprobson.hadoop;