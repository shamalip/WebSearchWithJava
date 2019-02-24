package ir.searchengine.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ir.searchengine.indexing.writables.InvertedIndexWritable;
import ir.searchengine.indexing.writables.PostWritable;


public class InvertedIdxReducer extends Reducer<Text,InvertedIndexWritable,Text,InvertedIndexWritable> {
	public void reduce(Text key, Iterable<InvertedIndexWritable> values, Context context) throws IOException, InterruptedException {
		HashMap<String,Integer> urlCountMap = new HashMap<String,Integer>();
		int totalCount = 0;
		for(InvertedIndexWritable cs : values) {
			String url =  cs.posts.get(0).getUrl();
			int count = cs.posts.get(0).getCount();
			totalCount = totalCount + count;
			urlCountMap.put(url, urlCountMap.getOrDefault(url, 0) + count);
		}
		InvertedIndexWritable res = new InvertedIndexWritable();
		List<PostWritable> pw = new ArrayList<PostWritable>();
		Iterator<Entry<String, Integer>> st = urlCountMap.entrySet().iterator();
		while(st.hasNext()) {
			PostWritable p = new PostWritable();
			Entry<String, Integer> next = st.next();
			p.setUrl(next.getKey());
			p.setCount(next.getValue());
			pw.add(p);			
		}			
		res.posts = pw;
		context.write(key, res);
	}

}