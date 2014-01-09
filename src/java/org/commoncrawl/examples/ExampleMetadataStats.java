package org.commoncrawl.examples;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
public class ExampleMetadataStats extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(ExampleMetadataStats.class);
  private static final String ARGNAME_INPATH = "-in";
  private static final String ARGNAME_OUTPATH = "-out";
  private static final String ARGNAME_CONF = "-conf";
  private static final String ARGNAME_OVERWRITE = "-overwrite";
  private static final String ARGNAME_MAXFILES = "-maxfiles";
  private static final String ARGNAME_NUMREDUCE = "-numreducers";
  private static final String FILEFILTER = "metadata-";
  
  protected static enum MAPPERCOUNTER {
    INVALID_URLS,
    EXCEPTIONS
  }

  /**
   * Mapping class that produces statistics about the Common Crawl corpus.
   */ 
  public static class ExampleMetadataStatsMapper
      extends Mapper<Text, Text, Text, LongWritable> {
    
    private Text outKey = new Text();
    private static final Text outKeyPReqTotal = new Text("Pages Requested\tTotal");
    private LongWritable outVal = new LongWritable(1);
    
    private String url;
    private String json;
    private String disposition;
    private String httpResult;
    private URI uri;
    private String host;
    private InternetDomainName domainName;
    private String mimeType;
    private String publicSuffix;
    private String charset;
    
    private static final JsonParser jsonParser = new JsonParser();
    private JsonObject jsonObj;
    
    @Override
    public void map(Text key, Text value, Context context) throws IOException {

      // key & value are "Text" right now ...
      url   = key.toString();
      json  = value.toString();
      disposition = "[no status]";
      httpResult = "[missing]";
      publicSuffix = "[none]";
      mimeType = "[missing]";
      charset = "[missing]";

      try {
 
        // See if the page has a successful HTTP code
        jsonObj = jsonParser.parse(json).getAsJsonObject();
 
        if (jsonObj.has("disposition")) {
          disposition = jsonObj.get("disposition").getAsString().trim().toUpperCase();
        }

        if (jsonObj.has("http_result")) {
          httpResult = jsonObj.get("http_result").getAsString().trim().toUpperCase();
        }

        // Output a basic page count
        context.write(outKeyPReqTotal, outVal);
        outKey.set("Pages Requested\t"+disposition);
        context.write(outKey, outVal);
        outKey.set("HTTP Code\t"+httpResult+" ("+disposition+")");
        context.write(outKey, outVal);

        // If the request was not successful, move to the next record
        if (!disposition.equals("SUCCESS")) {
          return;
        }

        // Gather the host name
        try {

          uri = new URI(url);
          host = uri.getHost();

          if (host == null || host.equals("")) {
            throw new URISyntaxException(url, "Unable to gather host or no host found");
          }

          // Gather the domain object
          domainName = InternetDomainName.from(host);

          // Output the TLD
          if (domainName.hasPublicSuffix()) {
            publicSuffix = domainName.publicSuffix().name().trim().toLowerCase();
          }

          outKey.set("TLD\t"+publicSuffix);
          context.write(outKey, outVal);

        }
        catch (URISyntaxException ex) {
          outKey.set("TLD\t[invalid URL]");
          context.write(outKey, outVal);
          context.getCounter(MAPPERCOUNTER.INVALID_URLS).increment(1);
        }
 
        if (jsonObj.has("mime_type")) {
          mimeType = jsonObj.get("mime_type").getAsString().trim().toLowerCase();
        }

        if (jsonObj.has("charset_detected")) {
          charset = jsonObj.get("charset_detected").getAsString().trim().toUpperCase();
        }
        
        if (jsonObj.has("download_size") == true) {
          context.write(new Text("Content Size\t"), new LongWritable(jsonObj.get("download_size").getAsInt()));
        }

        outKey.set("Charset\t"+charset);
        context.write(outKey, outVal);
        outKey.set("Type\t"+mimeType);
        context.write(outKey, outVal);

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
    System.out.println("\n  org.commoncrawl.examples.ExampleMetadataStats \n" +
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
    
    // Creates a new job configuration for this Hadoop job.
    Configuration conf = getConf();
    Job job = new Job(conf);
    job.setJarByClass(ExampleMetadataStats.class);
    job.setNumReduceTasks(numReducers);

    LOG.info("adding input path '" + inputPath + "'");
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
    job.setMapperClass(ExampleMetadataStats.ExampleMetadataStatsMapper.class);
    job.setCombinerClass(LongSumReducer.class);
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

    int res = ToolRunner.run(new Configuration(), new ExampleMetadataStats(), args);
    System.exit(res);
  }
}

