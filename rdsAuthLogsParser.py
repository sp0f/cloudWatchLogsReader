import boto3
import datetime
import time
from sys import exit
from os import path, remove

client = boto3.client('logs')
log_group = "/aws/rds/cluster/dv-mysql-cluster/audit"
log_file = '/var/log/rds-dv-mysql.log'
last_event_file = '/var/run/rdsAuthLogParser.lock'

now=datetime.datetime.now(datetime.timezone.utc)


kwargs = {
    'logGroupName': log_group,
    'limit': 10000,
    'startTime' : None,
    'filterPattern': '?FAILED_CONNECT ?CONNECT',
    'interleaved': True
}
if (path.exists(last_event_file)):
    print("Reading last event from lock file")
    tmp = f.readline()
    last_event = tmp
    # kwargs['startTime'] = int(last_event[:13])+1
    kwargs['startTime'] = last_event
    os.remove(last_event_file)

else:
    kwargs['startTime'] = int(now.timestamp()*1000000)
    last_event=str(kwargs['startTime'])


# read logs in infinite loop
while True:
    print("Last event: "+str(last_event))
    resp = client.filter_log_events(**kwargs)
    for event in resp['events']:
        # 1536652826127350,dv-mysql,alyjak,192.168.169.108,8426327,0,FAILED_CONNECT,,,1045
        timestamp, server_host, username, host, connection_id, query_id, operation, database, obj, retcode  = event['message'].split(",")
        event_time=datetime.datetime.fromtimestamp(int(timestamp)/1000000)
        log_line = event_time.strftime("%b %d %T") + " " + server_host + " rds: " + operation + " " + username + " " + host + " " + database + "\n"
        print(log_line)
        try:
            with  open(log_file, 'a') as f:
                f.write(log_line)
            # just a simple sanity check for next log pull
            if last_event<timestamp:
                last_event=timestamp
            else:
                print('last_event >= timestamp. It shouldn\'t happen')
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
        print('No new log events available. Waiting 5s before next pull')
        time.sleep(5) # no logs, let's wait