/**
 * 
 *  MapReduce Temperature Data Example
 *  
 *  Summary
 *  
 *  This is the Java MapReduce example taken from *'Hadoop The Definitive Guide 4th Edition' - Part 1 - Chapter 2.*
 *  
 *  The example uses Mapreduce framework to find the highest temperatures recorded per year from NCDC data files.
 *
 * Unix Version
 *
 * An example of the same Map/Reduce logic using unix command line tools is in the MapReduce_UnixTools directory (see the README file in that directory for details).
 *
 * Java Version
 *
 *  The Java version can be run in cluster mode (via yarn/HDFS) or in local mode (local filesystem) using the following maven goals: -
 * 
 * Cluster mode
 *
 * mvn exec:exec@run-cluster 
 *
 * Local mode
 *
 * mvn exec:exec@run-local 
 *
 * @author martinr
 *
 */
package net.martinprobson.hadoop.mr;