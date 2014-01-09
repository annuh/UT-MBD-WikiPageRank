register /home/participant/git/commoncrawl-examples/lib/*.jar;
register /home/participant/git/commoncrawl-examples/binladen.jar;  

register /home/participant/git/commoncrawl-examples/dist/lib/commoncrawl-examples-1.0.1.jar;

a = LOAD '/home/participant/data/*.arc.gz' USING org.commoncrawl.pig.ArcLoader() as (date, length:int, type, statuscode, ipaddress, url:chararray, html);

html = FILTER a BY (type == 'text/html');
checked = FOREACH html GENERATE url, udf.BinLaden(html) as danger;
binladen = FILTER checked BY (danger == true);
formatted = FOREACH binladen GENERATE url;
dump formatted;
