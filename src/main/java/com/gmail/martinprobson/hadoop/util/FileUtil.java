package com.gmail.martinprobson.hadoop.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtil {

    private static final Log LOG = LogFactory.getLog(FileUtil.class);
    
	public static boolean pathExists(Configuration conf, Path name) {
		boolean rc = true;
		try {
			FileSystem.get(conf).getFileStatus(name);
		} catch (FileNotFoundException e) {
				rc = false;
		} catch (IOException e) {
			LOG.error("Error getFileStatus() " + name,e);
			System.exit(2);
		}
		String msg = rc == true ? name + " exists" : name + " does not exist";
		LOG.info(msg);
		return rc;
	}

	public static void deletePath(Configuration conf, Path name) {
		LOG.info("Deleting " + name);
		try {
			FileSystem.get(conf).delete(name,true);
		} catch (IOException e) {
			LOG.error("Error deletePath() " + name,e);
			System.exit(2);
		}
	}
	
	public static String readFile(Configuration conf, Path fileName) {
		StringBuilder line = new StringBuilder();
		try {
			FSDataInputStream in = FileSystem.get(conf).open(fileName);   
			BufferedReader br = new BufferedReader(new InputStreamReader(in,Charset.defaultCharset()));			
			line = new StringBuilder();
			String tmp; 
			while ((tmp = br.readLine()) != null) 
				line.append(tmp + "\n");
			br.close();
		} catch (IOException e) {
			LOG.error("Error reading file: " + fileName, e);
			System.exit(2);
		}
		return(line.toString());
		
	}
	
	/**
	 * Read a file
	 * @param path - file to be read (URI)
	 * @param encoding - charset encoding.
	 * @return String representation of file.
	 */
	public static String readLocalFile(URI path, Charset encoding) 
			  throws IOException 
			{
			  byte[] encoded = Files.readAllBytes(Paths.get(path));
			  return new String(encoded, encoding);
			}

	/**
	 * Read a file
	 * @param path - file to be read (String)
	 * @param encoding - charset encoding.
	 * @return String representation of file.
	 */
	public static String readLocalFile(String path, Charset encoding) 
			  throws IOException, URISyntaxException
	{
			  return readLocalFile(new URI(path),encoding);
	}

}
