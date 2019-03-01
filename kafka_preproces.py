
import sh as sh
import re
from kafka import KafkaProducer
from json import dumps




producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))



def tail_F(some_file):
    while True:
        try:
            for line in sh.tail("-f", some_file, _iter=True):
                yield line
        except sh.ErrorReturnCode_1:
            yield None



HOST = r'^(?P<host>.*?)'
SPACE = r'\s'
IDENTITY = r'\S+'
USER = r'\S+'
TIME = r'(?P<time>\[.*?\])'
REQUEST = r'\"(?P<request>.*?)\"'
STATUS = r'(?P<status>\d{3})'
SIZE = r'(?P<size>\S+)'

REGEX = HOST+SPACE+IDENTITY+SPACE+USER+SPACE+TIME+SPACE+REQUEST+SPACE+STATUS+SPACE+SIZE+SPACE

def parser(log_line):
    match = re.search(REGEX,log_line)
    return ( (match.group('host'),
            match.group('time'), 
                      match.group('request') , 
                      match.group('status') ,
                      match.group('size')
                     )
                   )



data={}
index=0
for line in tail_F("access_log"): # read all lines already in the file
    l=(parser(line))
    data['index'] = 'node1_'+l[1]+str(index)
    data['host']=l[0]
    data['time']=l[1]
    data['request']=l[2]
    data['status']=l[3]
    data['size']=l[4]
        
    print(data)
    producer.send('log', value=data)
    index +=1






