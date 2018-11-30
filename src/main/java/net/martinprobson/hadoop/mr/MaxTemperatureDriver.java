package net.martinprobson.hadoop.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.martinprobson.hadoop.util.HDFSUtil;

/**
 * Driver class for the MaxTemperature Example.
 * 
 * <h4>Summary</h4>
 * <p>This class implements the standard Hadoop Tool interface and expects to be passed 
 * two arguments specifying the input and output directories as follows:
 * <p>
 * Running on a cluster via yarn: -
 * <p>
 * <code>
 * yarn jar jar_name MaxTemperatureDriver input_dir output_dir
 * </code>
 * <p>Running locally: -
 * <p>
 * <code>
 * hadoop --config local_config_file MaxTemperatureDriver input_dir output_dir
 * </code>
 * <p>Both run configurations are setup in the project pom.xml as goals <code>exec:exec@run-cluster</code> and 
 * <code>exec:exec@run-local</code> respectively.<p>
 * 
 * @author martinr
 *
 */
public class MaxTemperatureDriver extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(HDFSUtil.class);
	
    /**
     * Execute a MaxTemperature Map Reduce job.
     * 
     * @param args input directory, output directory.
     * @return exit code.
     * @throws Exception
     */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic_options] <input path> <output path>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Job job = Job.getInstance(getConf());
//TODO 14/10/2017 The test input file are in text format.....		
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("Max temperature");
		// Ensure that all files are read in the directory
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Check if output path exists and delete it if it does
		Path outputPath = new Path(args[1]);
		LOG.info("About to check if " + outputPath + " exists.");
		if (HDFSUtil.pathExists(getConf(),outputPath))
			HDFSUtil.deletePath(getConf(),outputPath);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

    /**
     * Execute a MaxTemperature Map Reduce job via command line.
     * 
     * @param args input directory, output directory.
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(),args);
		System.exit(exitCode);
	}
}
