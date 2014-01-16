package nl.utwente.wikipagerank.reducers;

import java.io.IOException;
import java.util.Iterator;

import nl.utwente.wikipagerank.mappers.WikiGraphMapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class WikiGraphReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(WikiGraphReducer.class);

	
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		StringBuilder sb = new StringBuilder("1.0\t");
		
		while(values.hasNext()){
			sb.append(values.next().toString());
			sb.append(",");
			
		}
		sb.setLength(sb.length() - 1);
		//LOG.info("Page: "+key.toString()); 
		
		output.collect(key, new Text(sb.toString()));
	}

}
