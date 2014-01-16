package nl.utwente.wikipagerank.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		String elements[] = value.toString().split("\t");
		String page = "";
		String pagerank = "1.0";
		String links = null;
		
		if(elements.length > 0){
			page = elements[0];
			// Mark page as an Efisxisting page 
			output.collect(new Text(page), new Text("!"));
		}
		
		if(elements.length < 3){
			return;
		}
		
		pagerank = elements[1];
		links = elements[2];
		
//		int pageTabIndex = value.find("\t");
//		int rankTabIndex = value.find("\t", pageTabIndex+1);
//		String page = Text.decode(value.getBytes(), 0, pageTabIndex);
//		String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);
//		// Skip pages with no links.
//		if(rankTabIndex == -1) return;
//		String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
		
		String[] allOtherPages = links.split(",");
		int totalLinks = allOtherPages.length;

		for (String otherPage : allOtherPages){
			Text pageRankTotalLinks = new Text(page + "\t" +pagerank + "\t" + totalLinks);
			output.collect(new Text(otherPage), pageRankTotalLinks);
		}

		// Put the original links of the page for the reduce output
		output.collect(new Text(page), new Text("|"+links));
	}
}