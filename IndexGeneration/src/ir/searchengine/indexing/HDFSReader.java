package ir.searchengine.indexing;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSReader {

	// TODO fetch from config.
	private static final String PATH_PREFIX = "hdfs://localhost:9000/hdfsstore/WEBPAGES_RAW/";
	URI fsURL = null;
	Configuration conf = null;

	public HDFSReader(String hdfsUrl) throws IOException {
		conf = new Configuration();
		conf.set("fs.defaultFS", hdfsUrl);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		fsURL = URI.create(hdfsUrl);
	}


	public KeyValueMapping readTSVToMap(String filePath) throws IOException{
		Map<String,String> fileMap = new HashMap<String,String>(); 
		List<String> ls = new ArrayList<String>();
		FileSystem fs = FileSystem.get(fsURL, conf);
		Path hdfsreadpath = new Path(filePath);
		FSDataInputStream bf = fs.open(hdfsreadpath);
		String dataRow = bf.readLine(); 
		String commaSeperatedList = "";
		while (dataRow != null){
			String[] dataArray = dataRow.split("\t");
			fileMap.put(dataArray[1] , dataArray[0]);
			dataRow = bf.readLine(); 
			if(ls.isEmpty()) {
				commaSeperatedList  =  PATH_PREFIX + dataArray[0];
			}else {
				commaSeperatedList  = commaSeperatedList +"," + PATH_PREFIX+dataArray[0];
			}
			ls.add(dataArray[0]);
		}
		bf.close();
		KeyValueMapping fmp = new KeyValueMapping();
		fmp.listOfFiles = ls;
		fmp.fileMap = fileMap;
		fmp.commaSeperatedList = commaSeperatedList;
		return fmp;
	}
}
