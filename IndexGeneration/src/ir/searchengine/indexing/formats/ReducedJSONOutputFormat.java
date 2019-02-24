package ir.searchengine.indexing.formats;

import java.util.List;

import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.zackehh.outputformat.JsonOutputFormat;

import ir.searchengine.indexing.writables.InvertedIndexWritable;
import ir.searchengine.indexing.writables.PostWritable;

public class ReducedJSONOutputFormat extends JsonOutputFormat<Text, InvertedIndexWritable>{
	
	@Override
	protected String convertKey(Text key) {
		return key.toString();
	}

	@Override
	protected JsonNode convertValue(InvertedIndexWritable value) {
		InvertedIndexValue invIdx = new InvertedIndexValue();
		List<PostWritable> posts = value.getPosts();
		Posting[] ps = new Posting[posts.size()];
		int i = 0;
		int total = 0;
		if(null != posts) {
			for(PostWritable posting: posts) {
				if(null != posting) {
					Posting p = new Posting();
					p.oc = posting.getCount();
					total = total + p.oc;
					p.url = posting.getUrl();
					ps[i] = p;					
				}
			}	
		}	
		invIdx.c = total;
		invIdx.ps = ps;
		return JsonNodeFactory.instance.pojoNode(invIdx);
	}

	/*@Override
	protected JsonNode convertValue(CustomWritable value) {
		InvertedIndexValue invIdx = new InvertedIndexValue();
		invIdx.count = value.getCount().get();
		PostingWritable[] postings = (PostingWritable[]) value.postings.get();
		Posting[] ps = new Posting[postings.length];
		int i = 0;
		if(null != postings) {
			for(PostingWritable posting: postings) {
				if(null != posting) {
					Posting p = new Posting();
					p.oc = posting.docCount.get();
					p.url = posting.url.toString();
					ps[i] = p;					
				}
			}	
		}	
		invIdx.ps = ps;
		return JsonNodeFactory.instance.pojoNode(invIdx);
	}*/

}
