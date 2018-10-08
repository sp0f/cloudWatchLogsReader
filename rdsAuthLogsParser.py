import boto3
import datetime
import time
from sys import exit
from os import path, remove

client = boto3.client('logs')
log_group = "/aws/rds/cluster/dv-mysql-cluster/audit"
log_file = '/var/ossec/logs/rds/rds-dv-mysql.log'
last_event_file = '/var/run/rdsAuthLogParser.lock'

now=datetime.datetime.now(datetime.timezone.utc)


kwargs = {
    'logGroupName': log_group,
    'limit': 10000,
    'logStreamNamePrefix': 'dv-mysql.audit.log',
}

# if last run exited with errors, start when it ends
if (path.exists(last_event_file)):
    print("Reading last event from lock file")
    tmp = f.readline()
    last_event = tmp
    kwargs['startTime'] = int(last_event)+1
    os.remove(last_event_file)

else:
    kwargs['startTime'] = int(now.timestamp()*1000)
    last_event=str(kwargs['startTime'])


# read logs in infinite loop
while True:
    resp = client.filter_log_events(**kwargs)
    for event in resp['events']:
        # print("Last event: "+str(last_event))
        timestamp, server_host, username, host, connection_id, query_id, operation, database, obj, retcode  = event['message'].split(",")
        event_time=datetime.datetime.fromtimestamp(int(timestamp)/1000000)
        # print("Timestamp: "+str(timestamp))
        log_line = event_time.strftime("%b %d %T") + " " + server_host + " rds: " + operation + " " + username + " " + host + " " + database + "\n"
        # print(log_line)
        try:
            with  open(log_file, 'a') as f:
                f.write(log_line)
        except:
            print("ERROR: Can't write to log file. Exiting")
            with open(last_event_file,'w') as f_event:
                f_event.write(last_event)
            exit(1)
        # just a simple sanity check for next log pull ... yes i know they'r strings
        if last_event<timestamp:
            last_event=timestamp
        elif last_event > timestamp:
            print("WARNING: last_event >= timestamp ["+log_line.rstrip()+"]. Restarting!")
            exit(1)
    try:
        kwargs['nextToken'] = resp['nextToken']
    except KeyError:
        kwargs['startTime'] = int(last_event[:13]) + 1  # that's a tricky part, AWS returns 16 digit timestamp but expect 13 digits
        # print('INFO: no new events available. Waiting 5s before next pull')
        time.sleep(5) # no logs, let's wait