package org.commoncrawl.pig;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.http.HttpException;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.commoncrawl.hadoop.mapred.ArcInputFormat;
import org.commoncrawl.hadoop.mapred.ArcRecord;


/**
 * A Pig Loader for Common Crawl ARC data. It currently provides
 * a Tuple with these 7 values, in this order:
 * <ol>
 * <li>archive date</li>
 * <li>content length</li>
 * <li>content type</li>
 * <li>HTTP response code</li>
 * <li>ip address</li>
 * <li>URL</li>
 * <li>the HTML (only if content-type == html, otherwise null)</li>
 * </ol>
 * 
 * The Loader can provide any data that is contained in an
 * {@link org.commoncrawl.hadoop.mapred.ArcRecord} by adding data
 * to the org.apache.pig.data.Tuple t in {@link #getNext()}.
 * 
 * @author Evert Lammerts <evert@apache.org>
 * @see org.commoncrawl.hadoop.mapred.ArcRecord
 *
 */
public class ArcLoader extends LoadFunc {

  private RecordReader<Text, ArcRecord> in;
  private TupleFactory mTupleFactory = TupleFactory.getInstance();
  private static final Logger LOG = Logger.getLogger(ArcLoader.class);
  
  // TODO: make this a configuration parameter, but no time.. 
private static final boolean USE_JSOUP = true;

  @Override
  public InputFormat<Text, ArcRecord> getInputFormat() throws IOException {
    return new ArcInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      ArcRecord value = null;
      while (value == null) {
        boolean notDone = in.nextKeyValue();
        if (!notDone) {
          return null;
        }
        value = in.getCurrentValue();
        try {
          value.getHttpResponse();
        } catch (HttpException e) {
          LOG.debug(e.getMessage());
          value = null;
        }
      }
      /**
       * This is where you can put other data in. Nasically anything you can find
       * in org.commoncrawl.hadoop.mapred.ArcRecord you can put into the Tuple. 
       */
      Tuple t = mTupleFactory.newTuple(7);
      t.set(0, value.getArchiveDate().toString());
      t.set(1, value.getContentLength());
      t.set(2, value.getContentType());
      try {
        t.set(3, value.getHttpStatusCode());
      } catch(HttpException e) {
        t.set(3, -1);
      }
      t.set(4, value.getIpAddress());
      t.set(5, value.getURL());
      if (value.getContentType().toLowerCase().contains("html")) {
        if (USE_JSOUP) {
    	  t.set(6, value.getParsedHTML().toString());
        }
        else {
      	  t.set(6, value.getHTML());
        }
        
      } else {
        t.set(6, null);
      }
      return t;
    } catch (InterruptedException e) {
      throw new IOException("Error getting input");
    } catch (NullPointerException npe) {
    	return null;
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void prepareToRead(RecordReader reader, PigSplit arg1)
      throws IOException {
    in = reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }

}
