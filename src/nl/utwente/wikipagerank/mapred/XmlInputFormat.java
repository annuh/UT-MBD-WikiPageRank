package nl.utwente.wikipagerank.mapred;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit,
			JobConf jobConf,
			Reporter reporter) throws IOException {
		return new XmlRecordReader((FileSplit) inputSplit, jobConf);
	}

	/**
	 * XMLRecordReader class to read through a given xml document to output xml
	 * blocks as records as specified by the start tag and end tag
	 * 
	 */
	public static class XmlRecordReader implements RecordReader<LongWritable,Text> {
		private final byte[] startTag;
		private final byte[] endTag;
		private final long start;
		private final long end;
		private long pos;
		private final CBZip2InputStream fsin;
		private final FSDataInputStream fsin2;
		private final DataOutputBuffer buffer = new DataOutputBuffer();
	    private long recordStartPos;


		public XmlRecordReader(FileSplit input, JobConf jobConf) throws IOException {
			startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
			endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");

			// open the file and seek to the start of the split
			//start = split.getStart();
			//end = start + split.getLength();
			//Path file = split.getPath();
			//FileSystem fs = file.getFileSystem(jobConf);
			//fsin = fs.open(split.getPath());
			//fsin.seek(start);
			
			
			
		      FileSplit split = (FileSplit) input;
		      start = split.getStart();
		      Path file = split.getPath();

//		      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(jobConf);
//		      CompressionCodec codec = compressionCodecs.getCodec(file);
		      FileSystem fs = file.getFileSystem(jobConf);
		      fsin2 = new FSDataInputStream(fs.open(file));
		      byte[] ignoreBytes = new byte[2];
		      fsin2.read(ignoreBytes); // "B", "Z" bytes from commandline tools
		      
		        fsin = new CBZip2InputStream(fsin2);
		        end = Long.MAX_VALUE;
//		      if (codec != null) {
//		        LOG.info("Reading compressed file " + file + "...");
//		        fs.open(split.getPath());
//		        //fsin2 = new FSDataInputStream(fs.open(file));
//		       // fsin = new CBZip2InputStream(fsin2);
//
//		        end = Long.MAX_VALUE;
//		      } else {
//		        LOG.info("Reading uncompressed file " + file + "...");
//		        FSDataInputStream fileIn = fs.open(file);
//
//		        fileIn.seek(start);
//		        //fsin = fileIn;
//
//		        end = start + split.getLength();
//		      }

		      recordStartPos = start;

		      // Because input streams of gzipped files are not seekable, we need to keep track of bytes
		      // consumed ourselves.
		      pos = start;
		}

		public boolean next(LongWritable key, Text value) throws IOException {
			if (pos < end) {
				if (readUntilMatch(startTag, false)) {
					recordStartPos = pos - startTag.length;
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							//key.set(pos);
							//value.set(buffer.getData(), 0, buffer.getLength());
							key.set(recordStartPos);
				              value.set(buffer.getData(), 0, buffer.getLength());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public Text createValue() {
			return new Text();
		}

		public long getPos() throws IOException {
			return pos;
		}

		public void close() throws IOException {
			fsin.close();
		}

		public float getProgress() throws IOException {
			//return (fsin.getPos() - start) / (float) (end - start);
		      return ((float) (pos - start)) / ((float) (end - start));

		}

		 private boolean readUntilMatch(byte[] match, boolean withinBlock)
			        throws IOException {
			      int i = 0;
			      while (true) {
			        int b = fsin.read();
			        // increment position (bytes consumed)
			        pos++;

			        // end of file:
			        if (b == -1)
			          return false;
			        // save to buffer:
			        if (withinBlock)
			          buffer.write(b);

			        // check if we're matching:
			        if (b == match[i]) {
			          i++;
			          if (i >= match.length)
			            return true;
			        } else
			          i = 0;
			        // see if we've passed the stop point:
			        if (!withinBlock && i == 0 && pos >= end)
			          return false;
			      }
			    
		 }
	}
}
