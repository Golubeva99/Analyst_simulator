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
import sys 
import os 


chat_id = # id чата

my_token = '' # токен бота
bot = telegram.Bot(token=my_token)
metrics_feed = ['users_feed', 'views', 'likes']
metrics_msg = ['users_msg', 'reciver_msg']


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.golubeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 12),
}

# Интервал запуска DAG'а каждые 15 минут
schedule_interval = '*/15 * * * *'

# Подключение к бд в clickhouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20220720',
    'user':'student', 
    'password':'dpo_python_2020'
}

query = """SELECT toStartOfFifteenMinutes(time) as ts,
                toDate(time) as date,
                formatDateTime(ts, '%R') as hm,
                uniqExact(user_id) as users_feed,
                countIf(user_id, action='view') as views,
                countIf(user_id, action='like') as likes
        FROM simulator_20220720.feed_actions
        WHERE time >= today()-1 and time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts"""

message_action = """SELECT
toStartOfFifteenMinutes(time) as ts,
toStartOfDay(time) as date,
formatDateTime(ts, '%R') as hm,
uniqExact(user_id) as users_msg,
count(reciever_id) as reciver_msg
FROM simulator_20220720.message_actions
WHERE ts >= today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts"""

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alert_golubeva():


    @task
    def extract_feed():
        df_feed = ph.read_clickhouse(query, connection=connection)
        return df_feed
    
    @task
    def extract_msg():
        df_msg = ph.read_clickhouse(message_action, connection=connection)
        return df_msg


    def check_anomaly(df, metric, a=4, n=5):
        # функция для поиска аномалий в данный (межквартильный размах)
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        df['up'] = df['q75'] + a*df['iqr']
        df['low'] = df['q25'] - a*df['iqr']

        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0      
        return is_alert, df

    @task()
    def run_alerts(df3, metrics):
        # функция системы алертов        
        for metric in metrics:
            df = df3[['ts', 'date', 'hm', metric]].copy()
            is_alert, df2 = check_anomaly(df, metric)

            if is_alert == 1 or True:
                msg = """Метрика {metric}:\n текущее значение {current_val:.2f}
                \n отклонение от предыдущего значения {last_val_diff:.2%}""".format(metric=metric,
                                                       current_val=df2[metric].iloc[-1],
                                                       last_val_diff=abs(1 - (df2[metric].iloc[-1]/df2[metric].iloc[-2])))
                sns.set(rc={'figure.figsize': (16,10)})
                plt.tight_layout()

                ax = sns.lineplot(x=df2['ts'], y=df2[metric], label='metric')
                ax = sns.lineplot(x=df['ts'], y=df2['up'], label='up')
                ax = sns.lineplot(x=df2['ts'], y=df2['low'], label='low')

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                #ax.set(xlable='time')
                #ax.set(ylable=metric)

                ax.set_title(metric)
                ax.set(ylim=(0, None))

                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    df_feed = extract_feed()
    df_msg = extract_msg()
    run_alerts(df_feed, metrics_feed)
    run_alerts(df_msg, metrics_msg)


dag_alert_golubeva = dag_alert_golubeva()