# Analyst simulator
Программа стажировки от https://karpov.courses. Программа позволяет попрактиковаться на реальных задачах с полным стеком технологий для анализа данных и настоящей инфраструктурой приложения, включавшего в себя ленту новостей и мессенджер. Курс в себя включал несколько разделов:
## Построение Dashboard
В рамках задачи были разработаны дашборды с основными продуктовыми метриками (DAU, MAU, WAU, Retention) и оперативные данные. Вся работа была выполнена в Superset.
## Анализ продуктовых метрик
В данных использования ленты новостей есть два типа пользователей: те, кто пришел через платный трафик source = 'ads', и те, кто пришел через органические каналы source = 'organic'. Задачей было проанализировать и сравнить Retention этих двух групп пользователей. Был сделан вывод о том, что ретеншен у этих групп пользователей практически не отличается.
## A/B-тесты
Мы убедились в том, что наша система сплитования работает корректно, и ключевая метрика не отличается между группами. Был проведен АА-тест на разных группах в течение недели. Были сформированы подвыборки без повторения и проведен t-тест. Далее был проведен AB-тест на двух группах пользователей (на одной из них был протестирован новый алгоритм рекомендаций). Были выбраны методы анализа и сравнивался CTR в двух группах.
## ETL-пайплайн (Airflow)
Был написан DAG в Airflow, который считатет вчерашние данные и записывает их в отдельную таблицу в ClickHouse.
## Автоматизация отчетности
Для автоматизации отчетности был создан бот в telegram, который с помощью Airflow каждый день присылает отчеты по ключевым метрикам в чат. 
## Система алертов
Чтобы контролировать ключевые метрики и данные по нашему приложению была реализована ситсема алертов, которая, в случае аномалий, присылает данные в чат.
___
