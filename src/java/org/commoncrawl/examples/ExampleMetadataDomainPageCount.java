package org.commoncrawl.examples;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.net.InternetDomainName;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * An example showing how to use the Common Crawl 'metadata' files to quickly
 * gather high level information about the corpus' content.
 * 
 * @author Chris Stephens <chris@commoncrawl.org>
 */
public class ExampleMetadataDomainPageCount extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(ExampleMetadataDomainPageCount.class);
  private static final String ARGNAME_INPATH = "-in";
  private static final String ARGNAME_OUTPATH = "-out";
  private static final String ARGNAME_CONF = "-conf";
  private static final String ARGNAME_OVERWRITE = "-overwrite";
  private static final String ARGNAME_MAXFILES = "-maxfiles";
  private static final String ARGNAME_NUMREDUCE = "-numreducers";
  private static final String FILEFILTER = "metadata-";
  
  protected static enum MAPPERCOUNTER {
    INVALID_URIS,
    INVALID_DOMAIN,
    MISSING_HTTPCODE,
    HTTP_SUCCESS,
    HTTP_FAIL,
    EXCEPTIONS
  }

  /**
   * Mapping class that produces the normalized domain name and a count of '1'
   * for every successfully retrieved URL in the Common Crawl corpus.
   */ 
  public static class ExampleMetadataDomainPageCountMapper
      extends Mapper<Text, Text, Text, LongWritable> {
    
    private final Text outKey = new Text();
    private static final LongWritable outVal = new LongWritable(1);
    
    String url;
    String json;
    URI uri;
    String host;
    InternetDomainName domainName;
    String domain;
    
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObj;

    @Override
    public void map(Text key, Text value, Context context)
        throws IOException {

      // key & value are "Text" right now ...
      url   = key.toString();
      json  = value.toString();

      try {

        // Get the base domain name
        uri = new URI(url);
        host = uri.getHost();

        if (host == null) {
          context.getCounter(MAPPERCOUNTER.INVALID_URIS).increment(1);
          return;
        }

        domainName = InternetDomainName.from(host);
        domain = domainName.topPrivateDomain().name();

        if (domain == null) {
          context.getCounter(MAPPERCOUNTER.INVALID_DOMAIN).increment(1);
          return;
        }

        // See if the page has a successful HTTP code
        jsonObj = jsonParser.parse(json).getAsJsonObject();

        if (jsonObj.has("http_result") == false) {
          context.getCounter(MAPPERCOUNTER.MISSING_HTTPCODE).increment(1);
          return;
        }

        if (jsonObj.get("http_result").getAsInt() == 200) {
          context.getCounter(MAPPERCOUNTER.HTTP_SUCCESS).increment(1);
          outKey.set(domain);
          context.write(outKey, outVal);
        }
        else {
          context.getCounter(MAPPERCOUNTER.HTTP_FAIL).increment(1);
        }
      }
      catch (IOException ex) {
        throw ex;
      }
      catch (Exception ex) {
        LOG.error("Caught Exception", ex); 
        context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
      }
    }
  }
  
  public void usage() {
    System.out.println("\n  org.commoncrawl.examples.ExampleMetadataDomainPageCount \n" +
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
    job.setJarByClass(ExampleMetadataDomainPageCount.class);
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
    job.setMapperClass(ExampleMetadataDomainPageCount.ExampleMetadataDomainPageCountMapper.class);
    job.setReducerClass(LongSumReducer.class);

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
    int res = ToolRunner.run(new Configuration(), new ExampleMetadataDomainPageCount(), args);
    System.exit(res);
  }
}

