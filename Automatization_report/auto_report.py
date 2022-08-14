import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.golubeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 8),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

# Подключение к бд в clickhouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20220720',
    'user':'student', 
    'password':'dpo_python_2020'
}

chat_id = # id чата можно узнать методом getUpdates
my_token = '' # токен бота
bot = telegram.Bot(token=my_token)


# Запросы к БД
# Лайки и просмотры за вчерашний день
query_lw = """       
        SELECT user_id,
                sum(action = 'like') as likes,
                sum(action = 'view') as views
        FROM simulator_20220720.feed_actions 
        WHERE toDate(time) = yesterday()
        GROUP BY user_id"""

# CTR за 7 дней
query_ctr = """
        SELECT toDate(time) as date,
            sum(action = 'like') as likes,
            sum(action = 'view') as views,
            COUNT(DISTINCT user_id) as users,
            likes / views as ctr
        FROM simulator_20220720.feed_actions 
        WHERE date between yesterday() - 7 and yesterday()
        GROUP BY date"""


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_golubeva():

    @task()
    def extract(query_lw):
        df_lw = ph.read_clickhouse(query_lw, connection=connection)
        return df_lw


    @task()
    def extract_data(query_ctr):
        df_ctr = ph.read_clickhouse(query_ctr, connection=connection)
        return df_ctr

    @task
    def transform(df_lw):
        dau = df_lw['user_id'].nunique()
        likes = df_lw['likes'].sum()
        views = df_lw['views'].sum()
        ctr = likes / views

        msg = f'''
        Данные по ленте новостей за вчерашний день:\n
        Количество активных пользователей за день (dau): {dau}.\n
        Всего просмотров: {views}.\n
        Всего лайков: {likes}.\n
        CTR: {ctr:.2f}.
        '''
        
        bot.sendMessage(chat_id=chat_id, text=msg)


    @task
    def plot_photo(df_ctr):
        figure, axis = plt.subplots(2, 2, figsize=(20,12))

        sns.lineplot(ax = axis[0, 0], data=df_ctr, x='date', y='ctr').set_title('CTR')
        axis[0, 0].grid()

        sns.lineplot(ax = axis[0, 1],  data=df_ctr, x='date', y='likes').set_title('Лайки')
        axis[0, 1].grid()

        sns.lineplot(ax = axis[1, 0], data=df_ctr, x='date', y='views').set_title('Просмотры')
        axis[1, 0].grid()

        sns.lineplot(ax = axis[1, 1], data=df_ctr, x='date', y='users').set_title('Активные пользователи')
        axis[1, 1].grid()

        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    


    df_lw = extract(query_lw)
    df_ctr = extract_data(query_ctr)
    transform(df_lw)
    plot_photo(df_ctr)
    


dag_report_golubeva = dag_report_golubeva()