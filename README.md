Norvig Web Data Science Award Examples
======================================

This is a fork of the original [CommonCrawl examples][0.1], adapted to be used
as a starting point for your entry to the [Norvig Web Data Science Award][0.2].

[0.1]: https://github.com/commoncrawl/commoncrawl-examples
[0.2]: http://norvigaward.github.com 


Getting started
---------------

We recommend using the virtual machine image as development environment as
described on the [contest website][1.1].

[1.1]: http://norvigaward.github.com/getstarted.html


Overview of the examples
------------------------

### Example MapReduce code

See the code for all examples [on Github][2.1].

All examples support the same arguments:

    org.commoncrawl.examples.Example*
                             -in <inputpath>
                             -out <outputpath>
                           [ -overwrite ]
                           [ -numreducers <number_of_reducers> ]
                           [ -conf <conffile> ]
                           [ -maxfiles <maxfiles> ]

Where:

* `-in`  
  Point to the path of your input files. You can use globbing if your Hadoop
  distribution supports it.
* `-out`  
  Point to the path to store the output files.
* `-overwrite`  
  If output path exists, this switch will allow the example to overwrite the
  existing directory.
* `-numreducers`  
  Set the maximum amount of reducers to run. Defaults to a single reducer.
* `-conf`  
  Path to additional configuration.
* `-maxfiles`  
  Maximum amount of files to process.

These examples are included:

* [org.commoncrawl.examples.ExampleArcMicroformat][2.2]  
An example showing how to analyze the CommonCrawl ARC web content files.

* [org.commoncrawl.examples.ExampleMetadataDomainPageCount][2.3]  
An example showing how to use the CommonCrawl 'metadata' files to quickly
gather high level information about the corpus' content.

* [org.commoncrawl.examples.ExampleMetadataStats][2.4]  
An example showing how to use the CommonCrawl 'metadata' files to quickly
gather high level information about the corpus' content.

* [org.commoncrawl.examples.ExampleTextWordCount][2.5]
An example showing how to use the CommonCrawl 'textData' files to efficiently
work with CommonCrawl corpus text content.

[2.1]: https://github.com/norvigaward/commoncrawl-examples/tree/master/src/java/org/commoncrawl/examples
[2.2]: https://github.com/norvigaward/commoncrawl-examples/blob/master/src/java/org/commoncrawl/examples/ExampleArcMicroformat.java
[2.3]: https://github.com/norvigaward/commoncrawl-examples/blob/master/src/java/org/commoncrawl/examples/ExampleMetadataDomainPageCount.java
[2.4]: https://github.com/norvigaward/commoncrawl-examples/blob/master/src/java/org/commoncrawl/examples/ExampleMetadataStats.java
[2.5]: https://github.com/norvigaward/commoncrawl-examples/blob/master/src/java/org/commoncrawl/examples/ExampleTextWordCount.java


### Build and package the examples

In the terminal you can build and package the examples by moving to the
commoncrawl-examples directory ~/git/commoncrawl-examples and run:

    $ ant

Inside Eclipse you can build the project by selecting "Project → Build Project"
from the menu bar. 

Both methods wil create a jar bundle in ~/git/commoncrawl-examples/dist/lib.


### Running the MapReduce examples

To run the an example on maximally 5 input files, open a shell and run:

    $ hadoop jar dist/lib/commoncrawl-examples-1.0.1.jar [EXAMPLECLASS] -in [INPUT] -out [OUTPUT] -maxfiles 5

For org.commoncrawl.examples.ExampleMetadataStats that would be

    $ hadoop jar dist/lib/commoncrawl-examples-1.0.1.jar org.commoncrawl.examples.ExampleMetadataStats -in [INPUT] -out [OUTPUT] -maxfiles 5

You can use this same command for each included example.

The Eclipse project includes a run configuration for the ExampleTextWordCount
example. You can select it from the "Run" menu entry. You can use this run
configuration as a template for other configurations.


### Example Pig script

 * [example.pig][2.6]  

An example counting the occurrences of HTTP status codes. You can run the pig
script from the terminal by moving to the examples directory and run:

    $ pig example.pig

[2.6]: https://github.com/norvigaward/commoncrawl-examples/blob/master/example.pig


Using the CommonCrawl ARC files in MapReduce and Pig
----------------------------------------------------

These examples come with an InputFormat for MapReduce and a Loader for Pig:

* [ArcInputFormat, ArcRecordReader, and ArcRecord][3.1]
* [ArcLoader][3.2]

[3.1]: https://github.com/norvigaward/commoncrawl-examples/tree/master/src/java/org/commoncrawl/hadoop/mapred
[3.2]: https://github.com/norvigaward/commoncrawl-examples/blob/master/src/java/org/commoncrawl/pig/ArcLoader.java

The above examples should show you how to load the CommonCrawl ARC files using
these classes.
