import boto3
import datetime
import time
from sys import exit
from os import path

client = boto3.client('logs')
log_group = "/aws/rds/cluster/dv-mysql-cluster/audit"
log_file = '/var/log/rds-dv-mysql.log'
last_event_file = '/var/run/rdsAuthLogParser.lock'

now=datetime.datetime.now(datetime.timezone.utc)


if (path.exists(last_event_file)):
    tmp=f.readline()
    last_event=int(tmp[:13])

else:
    last_event=int(now.timestamp()*1000)

kwargs = {
    'logGroupName': log_group,
    'limit': 10000,
    'startTime' : last_event,
    'filterPattern': '?FAILED_CONNECT ?CONNECT'
}

# read logs in infinite loop
while True:
    resp = client.filter_log_events(**kwargs)
    for event in resp['events']:
        # 1536652826127350,dv-mysql,alyjak,192.168.169.108,8426327,0,FAILED_CONNECT,,,1045
        timestamp, server_host, username, host, connection_id, query_id, operation, database, obj, retcode  = event['message'].split(",")
        event_time=datetime.datetime.fromtimestamp(int(timestamp)/1000000)
        log_line = event_time.strftime("%b %d %T") + " " + server_host + " rds: " + operation + " " + username + " " + host + " " + database + "\n"
        try:
            with  open(log_file, 'a') as f:
                f.write(log_line)
            # yust a simple sanity check for next log pull
            if last_event<timestamp:
                last_event=timestamp
            f.close()
        except:
            print("Error while writing data to log. Exiting")
            with open(last_event_file,'w') as f_event:
                f.write(last_event)
            exit(1)
    try:
        kwargs['nextToken'] = resp['nextToken']
    except KeyError:
        if last_event is not None:
            kwargs['startTime']=int(last_event[:13])+1 # that's a tricky part, AWS returns 16 digit timestamp but expect 13 digits
        time.sleep(5) # no logs, let's wait