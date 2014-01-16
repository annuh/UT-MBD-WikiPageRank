package nl.utwente.wikipagerank.reducers;


import java.io.IOException;
import java.util.Iterator;

import nl.utwente.wikipagerank.mappers.RankingMapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	private static final float damping = 0.85F;
	private static final Log LOG = LogFactory.getLog(PageRankReducer.class);

	
	public void reduce(Text page, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		boolean isExistingWikiPage = false;
		String[] split;
		float sumShareOtherPageRanks = 0;
		String links = "";
		String pageWithRank;
		//LOG.info("PR Reduce :"+page.toString());
		

		// For each otherPage:
		// - check control characters
		// - calculate pagearg0Rank share <rank> / count(<links>)
		// - add the share to sumShareOtherPageRanks
		while(values.hasNext()){
			pageWithRank = values.next().toString();

			if(pageWithRank.equals("!")) {
				isExistingWikiPage = true;
				continue;
			}

			if(pageWithRank.startsWith("|")){
				links = "\t"+pageWithRank.substring(1);
				continue;
			}
			//LOG.info(pageWithRank);
			split = pageWithRank.split("\\t");

			float pageRank = Float.valueOf(split[1]);
			int countOutLinks = Integer.valueOf(split[2]);

			sumShareOtherPageRanks += (pageRank/countOutLinks);
		}

		if(!isExistingWikiPage) return;
		float newRank = damping * sumShareOtherPageRanks + (1-damping);
		
		out.collect(page, new Text(newRank + links));
	}
}
