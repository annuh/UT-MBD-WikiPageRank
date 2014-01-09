package org.commoncrawl.examples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyCombiner<Key> extends Reducer<Key, LongWritable, Key, LongWritable> {
	private LongWritable result = new LongWritable();

	public void reduce(Key key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}