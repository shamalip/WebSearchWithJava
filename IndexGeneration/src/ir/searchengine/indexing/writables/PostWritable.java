package ir.searchengine.indexing.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class PostWritable implements Writable,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int count;
	String url;
	
	public PostWritable(){
		
	}
	
	public PostWritable(int c, String u){
		this.url = u;
		this.count = c;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		count = arg0.readInt();
		url = arg0.readUTF();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(count);
		arg0.writeUTF(url);		
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 31;
		result = result * prime +  count + (url == null ? 0 : url.hashCode());
		return result;
	}
	
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return url + "\t" + count; 
	}
	
	@Override
	public boolean equals(Object obj) {
		return this == obj;
	}

}