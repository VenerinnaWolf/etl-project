from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator  # оператор-заглушка (пустой оператор)
from airflow.operators.python import PythonOperator  # Python оператор. Позволяет определить пользовательскую функцию python и выполнить ее в рамках рабочего процесса airflow
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # оператор для запуска SQL из python

from datetime import datetime
import time

from core.constants import PATH, DB_CONNECTION
from core.functions import insert_data, log_start, log_end


# ---------------------
# DAG (сам ETL процесс)
# ---------------------

# Переменная с параметрами DAG по умолчанию
default_args = {
    "owner": "vizelikova",                  # владелец
    "start_date": datetime(2024, 12, 24),   # дата начала от которой следует начинать запускать DAG согласно расписанию
    "retries": 2                            # количество попыток повторить выполнение задачи при ошибке
}

with DAG(
    # Параметры DAG

    "insert_data",                          # название DAG
    default_args=default_args,              # параметры по умолчанию - присвоим им значение ранее определенной переменной
    description="Задание 1.1",              # описание DAG
    catchup=False,                          # не выполняет запланированные по расписанию запуски DAG, которые находятся раньше текущей даты (если start_date раньше, чем текущая дата)
    template_searchpath=[PATH],
    # schedule="0 0 * * *"
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

    # Разделитель
    split = DummyOperator(
        task_id="split"
    )

    # Окончание работы DAGа
    end = DummyOperator(
        task_id="end"
    )

    # ---------
    # Python операторы для чтения данных из csv файлов и загрузки сырых данных в схему stage:

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",                     # имя задачи
        python_callable=insert_data,                # функция python, которая будет выполняться (здесь это ранее написанная insert_data)
        op_kwargs={"table_name": "ft_balance_f"}    # входные параметры для этой функции
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_account_d"}
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_currency_d", "encoding": "cp1252"}  # у файла md_currency_d есть неизвестный символ для кодировки UTF-8, поэтому устанавливаем другую кодировку
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_ledger_account_s"}
    )

    # ---------
    # Операторы для запуска sql скриптов

    # Создание таблиц в схеме dsl по сырым данным из таблиц в схеме stage:

    sql_ft_balance_f = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f",                 # имя задачи
        conn_id=DB_CONNECTION,                      # id подключения к БД
        sql="sql/ft_balance_f.sql"                  # путь до SQL скриптов
    )

    sql_ft_posting_f = SQLExecuteQueryOperator(
        task_id="sql_ft_posting_f",
        conn_id=DB_CONNECTION,
        sql="sql/ft_posting_f.sql"
    )

    sql_md_account_d = SQLExecuteQueryOperator(
        task_id="sql_md_account_d",
        conn_id=DB_CONNECTION,
        sql="sql/md_account_d.sql"
    )

    sql_md_currency_d = SQLExecuteQueryOperator(
        task_id="sql_md_currency_d",
        conn_id=DB_CONNECTION,
        sql="sql/md_currency_d.sql"
    )

    sql_md_exchange_rate_d = SQLExecuteQueryOperator(
        task_id="sql_md_exchange_rate_d",
        conn_id=DB_CONNECTION,
        sql="sql/md_exchange_rate_d.sql"
    )

    sql_md_ledger_account_s = SQLExecuteQueryOperator(
        task_id="sql_md_ledger_account_s",
        conn_id=DB_CONNECTION,
        sql="sql/md_ledger_account_s.sql"
    )

    # --------
    # Операторы для логирования в БД

    task_log_start = PythonOperator(
        task_id="log_start",
        python_callable=log_start,
        provide_context=True,
    )

    task_log_end = PythonOperator(
        task_id="log_end",
        python_callable=log_end,
        provide_context=True,
    )

    # ---------------------
    # Порядок запуска задач
    # ---------------------

    # [] <- означает, что операторы будут выполняться параллельно
    # Т.к в очереди не могут находиться последовательно параллельные задачи, между ними нужно ставить одиночную задачу (в качестве разделителя поставим оператор-заглушку split)
    print(time.time())
    (
        start
        >> task_log_start
        >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s]
        >> split
        >> [sql_ft_balance_f, sql_ft_posting_f, sql_md_account_d, sql_md_currency_d, sql_md_exchange_rate_d, sql_md_ledger_account_s]
        >> task_log_end
        >> end
    )
