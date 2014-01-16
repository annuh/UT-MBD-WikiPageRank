package nl.utwente.wikipagerank.mappers;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RankingMapper extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> {
    
	private static final Log LOG = LogFactory.getLog(RankingMapper.class);
	
    public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter reporter) throws IOException {
        //String[] pageAndRank = getPageAndRank(key, value);
    	
        String pageAndRank[] = value.toString().split("\t");

        
        float parseFloat = Float.parseFloat(pageAndRank[1]);
        Text page = new Text(pageAndRank[0]);
        
        FloatWritable rank = new FloatWritable(parseFloat);
        
        output.collect(rank, page);
    }
    
}