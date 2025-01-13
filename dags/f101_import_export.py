from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator  # оператор-заглушка (пустой оператор)
from airflow.operators.python import PythonOperator  # Python оператор. Позволяет определить пользовательскую функцию python и выполнить ее в рамках рабочего процесса airflow
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # оператор для запуска SQL из python

from datetime import datetime
import time

from core.constants import PATH, DB_CONNECTION
from core.functions import insert_data, log_start, log_end, export_data

# ---------------------
# DAG (сам ETL процесс)
# ---------------------

# Переменная с параметрами DAG по умолчанию
default_args = {
    "owner": "vizelikova",                  # владелец
    "start_date": datetime(2025, 1, 13),   # дата начала от которой следует начинать запускать DAG согласно расписанию
    "retries": 2                            # количество попыток повторить выполнение задачи при ошибке
}

with DAG(
    # Параметры DAG

    "f101_import_export",                          # название DAG
    default_args=default_args,              # параметры по умолчанию - присвоим им значение ранее определенной переменной
    description="Задание 1.4",              # описание DAG
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

    # Экспорт таблицы из БД в csv файл
    export_f101 = PythonOperator(
        task_id='export_f101',
        python_callable=export_data,
        op_kwargs={"schema": "dm", "table_name": "dm_f101_round_f"},
        provide_context=True
    )

    # ---------------------
    # Порядок запуска задач
    # ---------------------

    # [] <- означает, что операторы будут выполняться параллельно
    # Т.к в очереди не могут находиться последовательно параллельные задачи, между ними нужно ставить одиночную задачу (в качестве разделителя поставим оператор-заглушку split)
    (
        start
        >> export_f101
        >> end
    )
