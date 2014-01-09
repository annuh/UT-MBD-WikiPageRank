package org.commoncrawl.examples;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * An example showing how to use the Common Crawl 'textData' files to efficiently
 * work with Common Crawl corpus text content.
 * 
 * @author Chris Stephens <chris@commoncrawl.org>
 */
public class ExampleTextWordCount extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(ExampleTextWordCount.class);
  private static final String ARGNAME_INPATH = "-in";
  private static final String ARGNAME_OUTPATH = "-out";
  private static final String ARGNAME_CONF = "-conf";
  private static final String ARGNAME_OVERWRITE = "-overwrite";
  private static final String ARGNAME_MAXFILES = "-maxfiles";
  private static final String ARGNAME_NUMREDUCE = "-numreducers";
  private static final String FILEFILTER = "textData-";
  
  protected static enum MAPPERCOUNTER {
    RECORDS_IN,
    EMPTY_PAGE_TEXT,
    EXCEPTIONS
  }

  /**
   * Perform a simple word count mapping on text data from the Common Crawl corpus.
   */
  protected static class ExampleTextWordCountMapper
      extends Mapper<Text, Text, Text, LongWritable> {

    private StringTokenizer tokenizer;
    private Text outKey = new Text();
    private LongWritable outVal = new LongWritable(1);

    @Override
    public void map(Text key, Text value, Context context)
        throws IOException {

      context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);

      try {
        tokenizer = new StringTokenizer(value.toString());
        if (!tokenizer.hasMoreTokens()) {
          context.getCounter(MAPPERCOUNTER.EMPTY_PAGE_TEXT).increment(1);
        } else {
          while (tokenizer.hasMoreTokens()) {
            outKey.set(tokenizer.nextToken());
            context.write(outKey, outVal);
          }
        }
      }
      catch (Exception ex) {
        LOG.error("Caught Exception", ex);
        context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
      }
    }
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
        } else if (args[i].equals(ARGNAME_MAXFILES)) {
          SampleFilter.setMax(Long.parseLong(args[++i]));
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

    // Create the Hadoop job.
    Configuration conf = getConf();
    Job job = new Job(conf);
    job.setJarByClass(ExampleTextWordCount.class);
    job.setNumReduceTasks(numReducers);

    // Scan the provided input path for ARC files.
    LOG.info("setting input path to '"+ inputPath + "'");
    SampleFilter.setFilter(FILEFILTER);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileInputFormat.setInputPathFilter(job, SampleFilter.class);

    // Delete the output path directory if it already exists and user wants to overwrite it.
    if (overwrite) {
      LOG.info("clearing the output path at '" + outputPath + "'");
      FileSystem fs = FileSystem.get(new URI(outputPath), conf);
      if (fs.exists(new Path(outputPath))) {
        fs.delete(new Path(outputPath), true);
      }
    }
    
    // Set the path where final output 'part' files will be saved.
    LOG.info("setting output path to '" + outputPath + "'");
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    // Set which InputFormat class to use.
    job.setInputFormatClass(SequenceFileInputFormat.class);

    // Set which OutputFormat class to use.
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // Set which Mapper and Reducer classes to use.
    job.setMapperClass(ExampleTextWordCount.ExampleTextWordCountMapper.class);
    //job.setReducerClass(LongSumReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setCombinerClass(MyCombiner.class);

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return 1;
    }
  }

  

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
      throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExampleTextWordCount(), args);
    System.exit(res);
  }
}

