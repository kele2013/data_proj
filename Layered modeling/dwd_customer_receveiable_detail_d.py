from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.utils import dates
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskMarker, ExternalTaskSensor

str_month = datetime.strftime(datetime.now() + timedelta(days=-1), format='%Y-%m-01')
last_month = datetime.strftime(datetime.now() + relativedelta(months=-1), format='%Y-%m')
last_3_month = datetime.strftime(datetime.now() + relativedelta(months=-3), format='%Y%m')
str_date = datetime.strftime(datetime.now() + timedelta(days=-1), format='%Y-%m-%d')
str_year = datetime.strftime(datetime.now() + timedelta(days=-1), format='%Y')

cur_partition= datetime.strftime(datetime.now() + timedelta(days=-1), format='p%Y%m')
insert_label= datetime.strftime(datetime.now(), format='%Y_%m_%d_%H_%M_%S')
starrocks_cluster=['192.168.18.111','192.168.18.112','192.168.18.113']
starrocks = random.choice(starrocks_cluster)


def default_options():
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2023, 2, 23 , 10 ,45 ),
        'retries': 1,
        'dagrun_timeout_sec': 60,
        'retry_delay': timedelta(seconds=5)
    }
    return default_args

def dwd_customer_receveiable_detail_d(dag):
    #t = "impala-shell -f /opt/script/sqls/sf_rt/dm_sf_customer_charge_detail_d.sql  --var str_year={} --var str_month={} --var str_date={} //".format(str_year,str_month,str_date)
   
   t = "mysql -h {} -u root -P9030 -ppoly2023 -D data_warehouse_rt < /opt/script/sqls/starrocks/dwd_customer_receveiable_detail_d.sql".format(starrocks)
   print("starrocks:",starrocks)
   print("command:",t)
   task = BashOperator(
        task_id='dwd_customer_receveiable_detail_d',
        bash_command=t,
        dag=dag)
   return task


with DAG(
        'dm_receveiable_rt',  # dag_id
        default_args=default_options(),
        schedule_interval="* * * * * */60",
)as d:
  dwd_customer_receveiable_detail_d = dwd_customer_receveiable_detail_d(d)

  dwd_customer_receveiable_detail_d