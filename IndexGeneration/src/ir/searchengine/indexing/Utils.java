package ir.searchengine.indexing;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class Utils {

	public static Map<String,String> loadConfigs(){
		Map<String,String> configs = new HashMap<String,String>();
		InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream("config.properties");
		try {
			Properties pr = new Properties();
			pr.load(inputStream);
			Iterator<String> it = pr.stringPropertyNames().iterator();
			while (it.hasNext())
			{
				String key = it.next();
				configs.put(key,pr.getProperty(key));
			}
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		return configs;
	} 
	
	public static KeyValueMapping readTSVFileToKeyValuePairs(String filePath, String hdfsUrl) throws IOException{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsUrl);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		URI fsURL = URI.create(hdfsUrl);
		Map<String,String> fileMap = new HashMap<String,String>(); 
		List<String> ls = new ArrayList<String>();
		FileSystem fs = FileSystem.get(fsURL, conf);
		Path hdfsreadpath = new Path(filePath);
		FSDataInputStream bf = fs.open(hdfsreadpath);
		String dataRow = bf.readLine(); // Read first line.
		String commaSeperatedList = "";
		while (dataRow != null){
			String[] dataArray = dataRow.split("\t");
			fileMap.put(dataArray[1] , dataArray[0]);
			dataRow = bf.readLine(); // Read next line of data.
			if(ls.isEmpty()) {
				commaSeperatedList  =  "hdfs://localhost:9000/hdfsstore/WEBPAGES_RAW/" + dataArray[0];
			}else {
				commaSeperatedList  = commaSeperatedList +"," + "hdfs://localhost:9000/hdfsstore/WEBPAGES_RAW/"+dataArray[0];
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
