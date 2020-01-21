from cassandra.cluster import Cluster
import re, gzip
import sys, os
from uuid import uuid1
import datetime
from cassandra.query import BatchStatement
 


if __name__ == '__main__':   
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    cluster = Cluster(['199.60.17.32', '199.60.17.65'])
    session = cluster.connect(keyspace)

    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    batch_size = 80

    for f in os.listdir(inputs):
    
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
        
            batch = BatchStatement()
    
            i = 0
            
            for line in logfile:
            
                match = re.search(line_re, line)
    
                if match:
        
                    match_line = re.split(line_re, line)
        
                    hostname = match_line[1]
                    date_new = datetime.datetime.strptime(match_line[2], '%d/%b/%Y:%H:%M:%S')
                    
                    path = match_line[3]
                    bytes = int(match_line[4])
                    
                    
                    batch.add(
                         """
                         INSERT INTO nasalogs (host, id, datetime, path, bytes) VALUES (%s, %s, %s, %s, %s)
          
                         """,
                         (hostname, uuid1(), date_new, path, bytes)
                         )
                    
                    
                    if i >= batch_size:
                        session.execute(batch)
                        batch.clear()
                        batch_iter = 0
                    i = i +1
                        