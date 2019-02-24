package ir.searchengine.indexing.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class InvertedIndexWritable implements Writable,Serializable{

	private static final long serialVersionUID = 1L;
	public List<PostWritable> posts; 
	
	public List<PostWritable> getPosts() {
		return posts;
	}

	public void setPosts(List<PostWritable> posts) {
		this.posts = posts;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		posts = new ArrayList<PostWritable>(size);
		for(int i = 0; i < size; i++) {
			PostWritable p = new PostWritable();
			p.readFields(in);
			posts.add(p);
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(posts.size());
	    for (PostWritable p : posts) {
	        p.write(out);
	    }		
	}	
}

