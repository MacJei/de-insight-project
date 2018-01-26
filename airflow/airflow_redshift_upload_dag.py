# airflow_redshift_upload_dag.py

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os
import pytz
# Datetime Variables/Other arguments
est_tz = pytz.timezone("America/New_York")
current_time = pytz.utc.localize(datetime.utcnow()).astimezone(est_tz)
upload_date = current_time.strftime('%Y-%m-%d')
upload_hour = current_time.strftime('%H')
upload_interval = int(current_time.strftime('%M'))/5
script_dir = os.getcwd() + '/scripts/'
# DAG Object

default_args = {
		'owner': 'insight-kenny',
		'depends_on_past': False,
		'start_date': datetime.now(),
		#'start_date': current_time,
		'retries': 3,
		'retry_delay': timedelta(minutes=1)
}

dag = DAG('redshift_upload', default_args=default_args, schedule_interval=timedelta(5))

upload_data = BashOperator(
	task_id='upload-to-redshift',
	bash_command='python {0}python/upload_to_redshift.py {1} {2} {3}'.format(script_dir, upload_date, upload_hour, str(upload_interval)),
	dag=dag
	)

