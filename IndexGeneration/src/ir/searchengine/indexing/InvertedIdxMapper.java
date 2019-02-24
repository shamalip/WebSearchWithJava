package ir.searchengine.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Entities;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;
import ir.searchengine.indexing.writables.InvertedIndexWritable;
import ir.searchengine.indexing.writables.PostWritable;

public class InvertedIdxMapper extends Mapper<Object, Text, Text, InvertedIndexWritable>{
	private Text word = new Text();
	InvertedIndexWritable c = new InvertedIndexWritable();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String fileName = getFileName(context);
		String text = extractTextFromHTML(value);
		StringTokenizer s = new StringTokenizer(text);
		while(s.hasMoreTokens()) {
			String token = s.nextToken();
			if(token.matches("[a-zA-Z0-9]+") && token.length() > 2) { //not just alpha numeric ???
				PostWritable p = new PostWritable(1,fileName);
				ArrayList<PostWritable> list = new ArrayList<PostWritable>();
				list.add(p);
				c.setPosts(list);
				word.set(token);
				context.write(word, c);
			}
		}   
	}

	private String extractTextFromHTML(Text value) {
		Document doc = Jsoup.parse(value.toString());
		doc.outputSettings().syntax(Document.OutputSettings.Syntax.html);
		doc.outputSettings().escapeMode(Entities.EscapeMode.base);
		doc.outputSettings().charset("UTF-8");
		doc.select("script,link,style,.hidden").remove();
		String text = Jsoup.clean(value.toString(), Whitelist.none());
		Elements metaTags = doc.getElementsByTag("meta");
		for (Element metaTag : metaTags) {
			String name = metaTag.attr("name");
			if("keywords".equalsIgnoreCase(name) || "description".equalsIgnoreCase(name)) {
				String content = metaTag.attr("content");
				text = text + " "+ content;
			}
		}
		Elements images = doc.getElementsByTag("img");
		for (Element image : images) {
			if(null != image.attr("alt")) {
				text = text + " "+ image.attr("alt");
			}
		}
		return text;
	}

	private String getFileName(Context context) {
		FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
		String fileName = fsFileSplit.getPath().getParent().getName() + "/" + fsFileSplit.getPath().getName();
		return fileName;
	}

}