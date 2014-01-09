register /home/participant/git/commoncrawl-examples/lib/*.jar; 

register /home/participant/git/commoncrawl-examples/dist/lib/commoncrawl-examples-1.0.1.jar;

a = LOAD '/home/participant/data/*.arc.gz' USING org.commoncrawl.pig.ArcLoader() as (date, length:int, type, statuscode, ipaddress, url:chararray, html);
log = FILTER a by (length >= 5000);
log1 = FOREACH log GENERATE url, length;

b = LOAD 'languages.dat' as (url:chararray, language:chararray);
dutch = FILTER b by (language == 'nl');

c = COGROUP log1 BY url INNER, dutch BY url INNER;
d = FOREACH c GENERATE $0;
dump d;
