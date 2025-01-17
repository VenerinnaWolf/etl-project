from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator  # оператор-заглушка (пустой оператор)
from airflow.operators.python import PythonOperator  # Python оператор. Позволяет определить пользовательскую функцию python и выполнить ее в рамках рабочего процесса airflow

from datetime import datetime

from core.constants import PATH
from core.functions import insert_data, transform_data

# ---------------------
# DAG (сам ETL процесс)
# ---------------------

# # Переопределяем переменную DB_CONNECTION под базу данных для проектного задания 2
# DB_CONNECTION = "postgres-project2"

# Переменная с параметрами DAG по умолчанию
default_args = {
    "owner": "vizelikova",                  # владелец
    "start_date": datetime(2025, 1, 13),   # дата начала от которой следует начинать запускать DAG согласно расписанию
    "retries": 2                            # количество попыток повторить выполнение задачи при ошибке
}

with DAG(
    # Параметры DAG

    "insert_data_2-2",                          # название DAG
    default_args=default_args,              # параметры по умолчанию - присвоим им значение ранее определенной переменной
    description="Задание 2.2. Загрузка данных на слой rd для обновления витрины dm.loan_holiday_info",  # описание DAG
    catchup=False,                          # не выполняет запланированные по расписанию запуски DAG, которые находятся раньше текущей даты (если start_date раньше, чем текущая дата)
    template_searchpath=[PATH],
    schedule=None                           # расписание в формате cron - с какой периодичностью будет автоматически выполняться DAG (None = DAG нужно запускать только мануально)
) as dag:
    # Тело DAG - задачи

    # ------------------
    # Задачи (операторы)
    # ------------------

    # ---------
    # Операторы-заглушки

    # Начало работы DAGа
    start = DummyOperator(
        task_id="start"
    )

    # Окончание работы DAGа
    end = DummyOperator(
        task_id="end"
    )

    # ---------
    # Python операторы:

    # Загрузка сырых данных из csv файла в таблицу в схеме stage
    load_deal_info = PythonOperator(
        task_id="load_deal_info",
        python_callable=insert_data,
        op_kwargs={"table_name": "deal_info", "delimiter": ",", "encoding": "windows-1251"}
    )

    load_product_info = PythonOperator(
        task_id="load_product_info",
        python_callable=insert_data,
        op_kwargs={"table_name": "product_info", "delimiter": ",", "encoding": "windows-1251"}
    )

    # Трансформация сырых данных из схемы stage в таблицу в схеме dm (запуск sql скрипта)
    transform_deal_info = PythonOperator(
        task_id="transform_deal_info",
        python_callable=transform_data,
        op_kwargs={"table_name": "deal_info"}
    )

    transform_product_info = PythonOperator(
        task_id="transform_product_info",
        python_callable=transform_data,
        op_kwargs={"table_name": "product_info"}
    )

    # ---------------------
    # Порядок запуска задач
    # ---------------------

    # [] <- означает, что операторы будут выполняться параллельно
    # Т.к в очереди не могут находиться последовательно параллельные задачи, между ними нужно ставить одиночную задачу (в качестве разделителя поставим оператор-заглушку split)
    (
        start
        >> load_deal_info
        >> load_product_info
        >> transform_deal_info
        >> transform_product_info
        >> end
    )
