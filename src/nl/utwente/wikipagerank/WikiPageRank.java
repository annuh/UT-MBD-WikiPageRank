package nl.utwente.wikipagerank;


import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import nl.utwente.wikipagerank.mappers.PageRankMapper;
import nl.utwente.wikipagerank.mappers.RankingMapper;
import nl.utwente.wikipagerank.mappers.WikiGraphMapper;
import nl.utwente.wikipagerank.mapred.XmlInputFormat;
import nl.utwente.wikipagerank.reducers.PageRankReducer;
import nl.utwente.wikipagerank.reducers.WikiGraphReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class WikiPageRank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(WikiPageRank.class);
	private static final String ARGNAME_INPATH = "-in";
	private static final String ARGNAME_OUTPATH = "-out";
	private static final String ARGNAME_CONF = "-conf";
	private static final String ARGNAME_OVERWRITE = "-overwrite";
	private static final String ARGNAME_MAXFILES = "-maxfiles";
	private static final String ARGNAME_NUMREDUCE = "-numreducers";

	private static NumberFormat nf = new DecimalFormat("00");

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikiPageRank(), args);
		System.exit(res);


		
	}
	
	public void usage() {
	    System.out.println("\n  org.commoncrawl.examples.ExampleTextWordCount \n" +
	                         "                           " + ARGNAME_INPATH +" <inputpath>\n" +
	                         "                           " + ARGNAME_OUTPATH + " <outputpath>\n" +
	                         "                         [ " + ARGNAME_OVERWRITE + " ]\n" +
	                         "                         [ " + ARGNAME_NUMREDUCE + " <number_of_reducers> ]\n" +
	                         "                         [ " + ARGNAME_CONF + " <conffile> ]\n" +
	                         "                         [ " + ARGNAME_MAXFILES + " <maxfiles> ]");
	    System.out.println("");
	    GenericOptionsParser.printGenericCommandUsage(System.out);
	  }

	/**
	 * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
	 *
	 * @param  args command line parameters, less common Hadoop job parameters stripped
	 *              out and interpreted by the Tool class.  
	 * @return      0 if the Hadoop job completes successfully, 1 if not. 
	 */
	@Override
	public int run(String[] args) throws Exception {

		String inputPath = null;
		String outputPath = null;
		String configFile = null;
		boolean overwrite = false;
		int numReducers = 1;

		// Read the command line arguments. We're not using GenericOptionsParser
		// to prevent having to include commons.cli as a dependency.
		for (int i = 0; i < args.length; i++) {
			try {
				if (args[i].equals(ARGNAME_INPATH)) {
					inputPath = args[++i];
				} else if (args[i].equals(ARGNAME_OUTPATH)) {
					outputPath = args[++i];
				} else if (args[i].equals(ARGNAME_CONF)) {
					configFile = args[++i];
				} else if (args[i].equals(ARGNAME_OVERWRITE)) {
					overwrite = true;
				} else if (args[i].equals(ARGNAME_NUMREDUCE)) {
					numReducers = Integer.parseInt(args[++i]);
				} else {
					LOG.warn("Unsupported argument: " + args[i]);
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				usage();
				throw new IllegalArgumentException();
			}
		}

		if (inputPath == null || outputPath == null) {
			usage();
			throw new IllegalArgumentException();
		}

		// Read in any additional config parameters.
		if (configFile != null) {
			LOG.info("adding config parameters from '"+ configFile + "'");
			this.getConf().addResource(configFile);
		}
		
		


		// Delete the output path directory if it already exists and user wants to overwrite it.
		if (overwrite) {
			LOG.info("clearing the output path at '" + outputPath + "'");
			FileSystem fs = FileSystem.get(new URI(outputPath),  getConf());
			if (fs.exists(new Path(outputPath))) {
				fs.delete(new Path(outputPath), true);
			}
		}

		// Set the path where final output 'part' files will be saved.
		LOG.info("setting output path to '" + outputPath + "'");
		
		WikiPageRank pageRank = new WikiPageRank();

		pageRank.runXmlParsing(inputPath, outputPath+"/iter00");

		int runs = 0;
		for (; runs < 5; runs++) {
			pageRank.runRankCalculation(outputPath+"/iter"+nf.format(runs), outputPath+"/iter"+nf.format(runs + 1));
		}

		pageRank.runRankOrdering(outputPath+"/iter"+nf.format(runs), outputPath+"/result");
		

		return 0;
	}

	public void runXmlParsing(String inputPath, String outputPath) throws IOException {
		JobConf conf = new JobConf(WikiPageRank.class);

		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		// Input / Mapper
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(XmlInputFormat.class);
		conf.setMapperClass(WikiGraphMapper.class);
		//conf.setNumMapTasks(1);

		// Output / Reducer
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setReducerClass(WikiGraphReducer.class);

		conf.setJarByClass(WikiPageRank.class);
		JobClient.runJob(conf);
		
	}

	private void runRankCalculation(String inputPath, String outputPath) throws IOException {
		JobConf conf = new JobConf(WikiPageRank.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);
		conf.setJarByClass(WikiPageRank.class);

		JobClient.runJob(conf);
	}

	private void runRankOrdering(String inputPath, String outputPath) throws IOException {
		JobConf conf = new JobConf(WikiPageRank.class);

		conf.setOutputKeyClass(FloatWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(RankingMapper.class);
		conf.setJarByClass(WikiPageRank.class);

		JobClient.runJob(conf);
	}

}
