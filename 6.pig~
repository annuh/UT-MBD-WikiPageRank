register /home/participant/git/commoncrawl-examples/lib/*.jar; 

register /home/participant/git/commoncrawl-examples/dist/lib/commoncrawl-examples-1.0.1.jar;

a = LOAD '/home/participant/data/*.arc.gz' USING org.commoncrawl.pig.ArcLoader() as (date, length:int, type, statuscode, ipaddress, url, html);

websites = FILTER a by (date matches 'Thu Feb 09 06:03:.*');

dates = GROUP websites by date;
cntd = foreach dates generate $0, SUM($1.length) as data; 

dump cntd;
