package ir.searchengine.indexing.formats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PostingWritable implements  WritableComparable<PostingWritable>{
	

	Text url;
	IntWritable rank;
	public Text getUrl() {
		return url;
	}

	public void setUrl(Text url) {
		this.url = url;
	}

	public IntWritable getRank() {
		return rank;
	}

	public void setRank(IntWritable rank) {
		this.rank = rank;
	}

	public IntWritable getDocCount() {
		return docCount;
	}

	public void setDocCount(IntWritable docCount) {
		this.docCount = docCount;
	}


	IntWritable docCount;
	
	public PostingWritable() {
		url = new Text();
		rank = new IntWritable();
		docCount = new IntWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		url.readFields(in);
		rank.readFields(in);
		docCount.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		url.write(out);
		rank.write(out);
		docCount.write(out);

	}

	@Override
	public int compareTo(PostingWritable o) {
		// TODO Auto-generated method stub
		return this.docCount.compareTo(o.docCount);
	}

	
	@Override
	public String toString() {
		return "\t" + url.toString() + "\t" + docCount.toString();
	}
}
