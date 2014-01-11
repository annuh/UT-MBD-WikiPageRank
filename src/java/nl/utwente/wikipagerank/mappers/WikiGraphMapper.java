package nl.utwente.wikipagerank.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WikiGraphMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outVal = new Text();

	private static final Log LOG = LogFactory.getLog(WikiGraphMapper.class);
	private static final Pattern wikiLinksPattern = Pattern.compile("\\[[.+?\\]]");

	/**
	 * Returns list of URLs in this page
	 * @param page
	 * @return list of URLs in this page
	 */
	private List<String> getUrls(String page){
		List<String> urls = new ArrayList<String>();
		Matcher m = wikiLinksPattern.matcher(page);
		while(m.find()){
			urls.add(m.group());
		}

		return urls;
	}

	private String parseTitle(String page){
		String title = "";
		final Pattern pattern = Pattern.compile("<title>(.+?)</title>");
		final Matcher matcher = pattern.matcher(page);
		matcher.find();
		title = matcher.group(1);
		return title;
	}

	private String getWikiPageFromLink(String aLink){
		if( aLink.length() < 3) return null;
		char firstChar = aLink.charAt(0);

		if( firstChar == '#') return null;
		if( firstChar == ',') return null;
		if( firstChar == '.') return null;
		if( firstChar == '&') return null;
		if( firstChar == '\'') return null;
		if( firstChar == '-') return null;
		if( firstChar == '{') return null;

		if( aLink.contains(":")) return null; // Matches: external links and translations links
		if( aLink.contains(",")) return null; // Matches: external links and translations links
		if( aLink.contains("&")) return null;

		int start = aLink.startsWith("[[") ? 2 : 1;
		int endLink = aLink.length();

		int pipePosition = aLink.indexOf("|");
		if(pipePosition > 0){
			endLink = pipePosition;
		}

		int part = aLink.indexOf("#");
		if(part > 0){
			endLink = part;
		}

		aLink = aLink.substring(start, endLink);
		aLink = aLink.replaceAll("\\s", "_");
		aLink = aLink.replaceAll(",", "");
		return aLink;
	}

	/* input key: offset
	 * input value: a wiki page
	 * output key: current wikipage name
	 * output value: out link
	 */
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {            

		String wikiPage = value.toString();
		LOG.info("Page: "+wikiPage); 
		Text outKey = new Text(parseTitle(wikiPage).replace(' ', '_'));
		output.collect(new Text(key.toString()), new Text("asdf"));
		List<String> urls = getUrls(wikiPage);
		for (int i = 0; i < urls.size(); i++) {
			String url = getWikiPageFromLink(urls.get(i));
			//if(url != null){
				//output.collect(outKey, new Text(url));
			//}
		}
	}
}

