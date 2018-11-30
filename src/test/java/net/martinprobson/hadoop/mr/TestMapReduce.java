package net.martinprobson.hadoop.mr;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.martinprobson.hadoop.util.HDFSUtil;




public class TestMapReduce {
	
	/**
	 * The output directory for results.
	 */
	public static final String TEST_OUTPUT_DIR = "mini_ncdc_output";
	
	/**
	 * The Configutation object used for the test.
	 */
	private Configuration conf;
	
	/**
	 * Clean up output directory.
	 */
	@Before
	public void setUp() throws Exception {
		conf = new Configuration();
	}
	
	
	@Test
	public void testMapReduce() throws Exception {
		
		// The input data dir needs to be in the Maven test/resources directory.
		Path input  = new Path(TestMapReduce.class.getResource("/mini_ncdc").toString());
		Path output = new Path(TEST_OUTPUT_DIR);
				
		HDFSUtil.deletePath(conf,output);
		
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[] {
				input.toString(),
				output.toString()
		});
		assertTrue(exitCode == 0 );
		
		URI expectedResultsFile = TestMapReduce.class.getResource("/TestMapReduce_expected_results.txt").toURI();
		String expectedResults = HDFSUtil.readLocalFile(expectedResultsFile,Charset.defaultCharset());
		String actualResults = HDFSUtil.readFile(conf,new Path(TEST_OUTPUT_DIR + "/part-r-00000"));
		assertTrue(expectedResults.equals(actualResults));
		//checkoutput(conf,output);
		
		
	}
	
	/**
	 * Delete the output directory.
	 */
	@After
	public void tearDown() throws Exception {
		HDFSUtil.deletePath(conf,new Path(TEST_OUTPUT_DIR));
	}


}
