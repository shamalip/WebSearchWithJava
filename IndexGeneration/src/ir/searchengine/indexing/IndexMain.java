package ir.searchengine.indexing;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ir.searchengine.indexing.formats.ReducedJSONOutputFormat;
import ir.searchengine.indexing.writables.InvertedIndexWritable;

public class IndexMain {

	private static Map<String,String> configs = Utils.loadConfigs();
	public static void main(String[] args) throws Exception {
		Job job = initJob(); 
		/** Read files from hdfs **/
		HDFSReader hr = new HDFSReader(configs.get("HDFS_URI"));
		KeyValueMapping urlMap = hr.readTSVToMap(configs.get("HDFS_BASE_PATH") + configs.get("FILE_URL_MAPPING_FILE"));
		String listOfFiles = urlMap.commaSeperatedList;		
		FileInputFormat.setInputPaths(job, listOfFiles);
		FileOutputFormat.setOutputPath(job, new Path(configs.get("HDFS_BASE_PATH") + "/outputB"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static Job initJob() throws IOException {
		Job job = createJob();
		job.setJarByClass(IndexMain.class);
		job.setMapperClass(InvertedIdxMapper.class);
		job.setCombinerClass(InvertedIdxReducer.class);
		job.setReducerClass(InvertedIdxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InvertedIndexWritable.class);
		job.setOutputFormatClass(ReducedJSONOutputFormat.class);
		return job;
	}

	private static Job createJob() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", configs.get("HDFS_URI"));
		conf.addResource(new Path(configs.get("SITE_URL")));
		conf.addResource(new Path(configs.get("SITE_XML")));
		conf.addResource(new Path(configs.get("MAPRED_SITE_XML")));
		Job job = Job.getInstance(conf, "UCI Domain Indexer");
		return job;
	}

}
