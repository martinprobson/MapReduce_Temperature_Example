package com.gmail.martinprobson.hadoop.util;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class TestFileUtil {

	private static final Log log = LogFactory.getLog(TestFileUtil.class);
	private static final Configuration localConf;
	private static final Configuration hdfsConf;
	
	/**
	 * The Configuration we are currently testing against.
	 * (local or HDFS).
	 */
	private Configuration conf;
	
	/**
	 * The FileSystem we are currently testing against.
	 * (local or HDFS).
	 */
	private FileSystem	 fs;

	static {
		localConf = new Configuration();
		hdfsConf  = new Configuration();
		if (System.getProperty("test.build.data") == null)
			System.setProperty("test.build.data", "/tmp");
		try {
			new MiniDFSCluster.Builder(hdfsConf).build();
			log.info("After local FS_DEFAULT_NAME_KEY = " + localConf.get(FS_DEFAULT_NAME_KEY));
			log.info("After HDFS  FS_DEFAULT_NAME_KEY = " + hdfsConf.get(FS_DEFAULT_NAME_KEY));
		} catch (IOException e) {
			throw new RuntimeException("Failure to get MiniDFSCluster",e);
		}
	}

	
	@Parameters
	public static Collection<Configuration> configs() {
		Collection<Configuration> cfg = new ArrayList<>();
		cfg.add(localConf);
		cfg.add(hdfsConf);
		
		return cfg;
	}
	
	public TestFileUtil(Configuration conf) throws IOException {
		this.conf = conf;
		this.fs = FileSystem.get(conf);
		log.info("Testing FileUtil with FileSystem set to: " + fs.getUri() );
	}
	

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		log.info("In setupBeforeClass");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		log.info("In tearDownAfterClass");
		
	}

	@Test
	public void testPathExists() throws IOException {
		Path exists = new Path("/tmp/path_exists");
		OutputStream out = fs.create(exists);
		out.write("test-content".getBytes(Charset.defaultCharset()));
		out.close();
		assertTrue(FileUtil.pathExists(conf, exists));
		
		Path notExists = new Path("/tmp/path_does_not_exist");
		FileUtil.deletePath(conf, notExists);
		assertFalse(FileUtil.pathExists(conf, notExists));
	}

	@Test
	public void testDeletePath() throws IOException {
		Path exists    = new Path("/tmp/testDeletePath");
		Path notExists = new Path("/tmp/testDeletePath2");
		OutputStream out = fs.create(exists);
		out.close();
		FileUtil.deletePath(conf, exists);
		assertFalse(FileUtil.pathExists(conf, exists));
		FileUtil.deletePath(conf, notExists);
		assertFalse(FileUtil.pathExists(conf, notExists));
	}

	@Test
	public void testReadFile() throws IOException {
		Path exists = new Path("/tmp/testReadFile");
		OutputStream out = fs.create(exists);
		out.write("test-content".getBytes(Charset.defaultCharset()));
		out.close();
		String content = FileUtil.readFile(conf,exists);
		assertTrue(content.equals("test-content\n"));
	}

	@Test
	public void testReadLocalFileURICharset() throws URISyntaxException, IOException {
		URI expectedResultsFile = TestFileUtil.class.getResource("/testReadLocalFile.txt").toURI();		
		String content = FileUtil.readLocalFile(expectedResultsFile, Charset.defaultCharset());
		assertTrue(content.equals("test-content\n"));
	}

	@Test
	public void testReadLocalFileStringCharset() throws IOException, URISyntaxException {
		String expectedResultsFile = TestFileUtil.class.getResource("/testReadLocalFile.txt").toString();		
		String content = FileUtil.readLocalFile(expectedResultsFile, Charset.defaultCharset());
		assertTrue(content.equals("test-content\n"));
	}

}
