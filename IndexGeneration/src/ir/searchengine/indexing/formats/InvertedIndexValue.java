package ir.searchengine.indexing.formats;


public class InvertedIndexValue {
	Posting[] ps;
	Integer c;	
	
	public Posting[] getPs() {
		return ps;
	}
	public void setPs(Posting[] ps) {
		this.ps = ps;
	}
	public Integer getC() {
		return c;
	}
	public void setC(Integer count) {
		this.c = count;
	}
}
