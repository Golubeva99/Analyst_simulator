# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Подключение к бд в clickhouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20220720',
    'user':'student', 
    'password':'dpo_python_2020'
}

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw',
    'password':'656e2b0c9c'}



# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.golubeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 6),
}




# Запросы в БД
query_action = """SELECT toDate(time) AS event_date, user_id AS user,gender, age, os,count(action='view') AS views,
                        count(action='like') AS likes
                    FROM simulator_20220720.feed_actions 
                    WHERE event_date = today()-1
                    GROUP BY event_date, user, gender, age, os
                    format TSVWithNames
                    """



query_message = """SELECT
            event_date, user, gender, age, os,
            messages_received, messages_sent,  users_received, users_sent
        FROM
        (SELECT 
            toDate(time) AS event_date, 
            user_id AS user,
            gender, age, os,
            count() AS messages_sent, uniq(reciever_id) AS users_sent
        FROM simulator_20220720.message_actions
        WHERE event_date = today()-1
        GROUP BY event_date, user, gender, age, os) t1
        FULL OUTER JOIN
        (SELECT 
            toDate(time) AS event_date, 
            reciever_id AS user,
            gender, age, os,
            count() AS messages_received, uniq(user_id) AS users_received
        FROM simulator_20220720.message_actions
        WHERE event_date = today()-1
        GROUP BY event_date, user, gender, age, os) t2
        USING (user)"""



# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result



# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_a_golubeva():

    @task()
    def extract():
        query_action = """SELECT toDate(time) AS event_date, user_id AS user,gender, age, os,countIf(action='view') AS views,
                        countIf(action='like') AS likes
                    FROM simulator_20220720.feed_actions 
                    WHERE event_date = today()-1
                    GROUP BY event_date, user, gender, age, os
                    format TSVWithNames
                    """
        df_action = ch_get_df(query=query_action)
        return  df_action
    
    @task()
    def extract_data():    
        query_message = """SELECT
                            event_date, user, gender, age, os,
                            messages_received, messages_sent,  users_received, users_sent
                        FROM
                        (SELECT 
                            toDate(time) AS event_date, 
                            user_id AS user,
                            gender, age, os,
                            count() AS messages_sent, uniq(reciever_id) AS users_sent
                        FROM simulator_20220720.message_actions
                        WHERE event_date = today()-1
                        GROUP BY event_date, user, gender, age, os) t1
                        FULL OUTER JOIN
                        (SELECT 
                            toDate(time) AS event_date, 
                            reciever_id AS user,
                            gender, age, os,
                            count() AS messages_received, uniq(user_id) AS users_received
                        FROM simulator_20220720.message_actions
                        WHERE event_date = today()-1
                        GROUP BY event_date, user, gender, age, os) t2
                        USING (user)
                    format TSVWithNames
                    """
        df_message = ch_get_df(query=query_message)
        return  df_message

    
    @task
    def merge_all(df_action, df_message):
        df_all = df_action.merge(
            df_message, how='outer', 
            on=['event_date', 'user', 'gender', 'age', 'os']).dropna()
        df_all[['views', 'likes','messages_received', 'messages_sent','users_received', 'users_sent']] = df_all[['views','likes','messages_received', 'messages_sent','users_received', 'users_sent']].astype(int)
        return df_all


    
    @task
    def slice_gender(df_all):
        sl_gender = df_all[['event_date', 'gender', 'views', 'likes', 'messages_sent','messages_received']]\
            .groupby(['event_date','gender'])\
            .sum()\
            .reset_index()
        return sl_gender

    @task
    def slice_age(df_all):
        sl_age = df_all[['event_date', 'age', 'views', 'likes', 'messages_sent','messages_received']]\
            .groupby(['event_date','age'])\
            .sum()\
            .reset_index()
        return sl_age
    
    @task
    def slice_os(df_all):
        sl_os = df_all[['event_date', 'os', 'views', 'likes', 'messages_sent','messages_received']]\
            .groupby(['event_date','os'])\
            .sum()\
            .reset_index()
        return sl_os
    
    @task
    def load(sl_gender, sl_age, sl_os):
        sl_gender.rename(columns={'gender': 'dimension_value'}, inplace = True)
        sl_gender.insert(1, 'dimension', 'gender')

        sl_age.rename(columns={'age': 'dimension_value'}, inplace = True)
        sl_age.insert(1, 'dimension', 'age')

        sl_os.rename(columns={'os': 'dimension_value'}, inplace = True)
        sl_os.insert(1, 'dimension', 'os')

        
        general_table = pd.concat([sl_gender, sl_age, sl_os])
        
        
        # Запрос для создания таблицы
        query_test = """CREATE TABLE IF NOT EXISTS test.etl_table_ag_golubeva (
                    event_date Date,
                    dimension String,
                    dimension_value String,
                    views UInt64,
                    likes UInt64,
                    messages_received UInt64,
                    messages_sent UInt64,
                    users_received UInt64,
                    users_sent UInt64
                )
                ENGINE = MergeTree()
                ORDER BY event_date
                """
        
        
        # Загрузка датафрейма в базу данных test Clickhouse
        ph.execute(connection=connection_test, query=query_test)
        ph.to_clickhouse(general_table, table='etl_table_ag_golubeva', connection=connection_test, index=False)
        
    
    df_action = extract()
    df_message = extract_data()
    df_all = merge_all(df_action, df_message)
    sl_gender = slice_gender(df_all)
    sl_age = slice_age(df_all)
    sl_os = slice_os(df_all)
    load(sl_gender, sl_age, sl_os)
    
dag_a_golubeva = dag_a_golubeva()