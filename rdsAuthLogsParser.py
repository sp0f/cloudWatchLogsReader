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
    'logStreamNamePrefix': 'dv-mysql.audit.log',
    'interleaved': True
}

last_event=int(now.timestamp()*1000000)
# if (path.exists(last_event_file)):
#     print("Reading last event from lock file")
#     tmp = f.readline()
#     last_event = tmp
#     kwargs['startTime'] = int(last_event)+1
#     os.remove(last_event_file)
#
# else:
#     kwargs['startTime'] = int(now.timestamp()*1000000)
#     last_event=str(kwargs['startTime'])

print("Start event timestamp: "+str(last_event))
kwargs['startTime'] = last_event
resp = client.filter_log_events(**kwargs)
kwargs.pop('startTime')

# read logs in infinite loop
while True:
    for event in resp['events']:
        print("Last event: "+str(last_event))
        # 1536652826127350,dv-mysql,alyjak,192.168.169.108,8426327,0,FAILED_CONNECT,,,1045
        timestamp, server_host, username, host, connection_id, query_id, operation, database, obj, retcode  = event['message'].split(",")
        event_time=datetime.datetime.fromtimestamp(int(timestamp)/1000000)
        log_line = event_time.strftime("%b %d %T") + " " + server_host + " rds: " + operation + " " + username + " " + host + " " + database + "\n"
        print("Timestamp: " + str(timestamp))
        print(log_line)
        try:
            with  open(log_file, 'a') as f:
                f.write(log_line)

            # just a simple sanity check for next log pull
        except:
            print("ERROR: can't write data to log file. Exiting")
            # with open(last_event_file,'w') as f_event:
            #     f.write(last_event)
            f.close()
            exit(1)
        int_timestamp=int(timestamp)
        if last_event<int_timestamp:
            last_event=int_timestamp
        elif last_event>int_timestamp:
            print("WARNING: last_event > timestamp. It shouldn't happen")
        f.close()
    try:
        kwargs['nextToken'] = resp['nextToken']
    except KeyError:
        # if last_event is not None:
        #     kwargs['startTime']=int(last_event)+1 # that's a tricky part, AWS returns 16 digit timestamp but expect 13 digits
        # print('No new log events available. Waiting 5s before next pull')
        time.sleep(5) # no logs, let's wait
        resp = client.filter_log_events(**kwargs)