import boto3
import psycopg2 as pg
import os
import datetime
import sys
import pytz

# Set time variables 
est_tz = pytz.timezone("America/New_York")
current_time = pytz.utc.localize(datetime.datetime.utcnow()-datetime.timedelta(minutes=1)).astimezone(est_tz)
upload_date = current_time.strftime('%Y-%m-%d')
upload_hour = int(current_time.strftime('%H'))
upload_interval = int(current_time.strftime('%M'))/5

# Set Redshift environment variables
rs_user = os.getenv('REDSHIFT_USER')
rs_pass = os.getenv('REDSHIFT_PW')
rs_host = os.getenv('REDSHIFT_HOST')
rs_iam = os.getenv('REDSHIFT_IAM')

if __name__ == "__main__":

    redshift_config = {
        'dbname': 'dev',
        'host': rs_host,
        'port': 5439,
        'user': rs_user,
        'password': rs_pass
    }

    # Connect to Redshift
    conn = pg.connect(
        dbname=redshift_config['dbname'],
        user=redshift_config['user'],
        password=redshift_config['password'],
        host=redshift_config['host'],
        port=redshift_config['port']
    )
    cur = conn.cursor()    
    try:
        #upload_date = sys.argv[1]
        #upload_hour = sys.argv[2]
        #upload_interval = sys.argv[3]
        cur.execute("""COPY web_event_logs
                    from 's3://insight-spark-stream-files/event_logs/upload_date={0}/upload_hour={1}/upload_interval={2}/'
                    iam_role '{3}'
                    csv
                    delimiter as '|'
                    timeformat 'auto'
                    """.format(upload_date, upload_hour, upload_interval, rs_iam))
        cur.execute("""COPY web_event_agg
                    from 's3://insight-spark-stream-files/event_aggs/upload_date={0}/upload_hour={1}/upload_interval={2}/'
                    iam_role '{3}'
                    csv
                    delimiter as '|'
                    dateformat 'auto'
                    """.format(upload_date, upload_hour, upload_interval, rs_iam))

	print "Upload Complete."
    except IndexError as e:
        print "Job failed."
        print e
        print """
        Not enough arguements. Please input <upload_date> <upload_hour> <upload_interval>
        <upload_interval is a value from 1-12 representing a 5 minute block of time in an hour."""
    except pg.InternalError as e2:
        print "File not loaded yet."
        print e2

    conn.commit()
    cur.close()
    conn.close()
