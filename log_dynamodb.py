import boto3
import re



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


session = boto3.Session(profile_name="dynamodb_s3")

def log_to_dynamodb():
    log_table = session.resource('dynamodb',region_name='us-east-1').Table('Log_store')
    f = open('access_log/access_log', 'r')
    data={}
    index=0
    for line in f: # read all lines already in the file
        
        l=(parser(line))
        data['index'] = 'node1_'+l[1]+str(index)
        data['host']=l[0]
        data['time']=l[1]
        data['request']=l[2]
        data['status']=l[3]
        data['size']=l[4]
        
        print("inserting row "+str(index+1))
        log_table.put_item(Item=data)
        
        index +=1
        
    return "Inserted successfully"



log_to_dynamodb()



