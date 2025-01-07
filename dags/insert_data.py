from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator  # оператор-заглушка (пустой оператор)
from airflow.operators.python import PythonOperator  # Python оператор. Позволяет определить пользовательскую функцию python и выполнить ее в рамках рабочего процесса airflow
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # оператор для запуска SQL из python
from airflow.providers.postgres.hooks.postgres import PostgresHook  # связь с БД PostgreSQL
from airflow.configuration import conf  # конфигурация нашего Airflow (настройки)
from airflow.models import Variable  # импортируем переменные, которые мы вводили в настройках Airflow (http://localhost:8080/variable/)

import pandas  # библиотека для загрузки файлов из БД
from datetime import datetime
import time

# ----------
# Переменные и константы
# ----------

# Чтобы иметь возможность запускать sql скрипты из Airflow, зададим параметр template_searchpath - шаблон по которому ищется начальный путь до файлов.
# Вытаскиваем из настроек Airflow переменную my_path - путь до используемых файлов. Сейчас она имеет значение /files/
PATH = Variable.get("my_path")
# Добавляем этот путь в шаблон
conf.set("core", "template_searchpath", PATH)

# DB_CONNECTION = "postgres-ht"  # Связь с базой данных для дз "Практика"
DB_CONNECTION = "postgres-project"  # Связь с базой данных для проекта

# Получаем настройки подключения для PostgreSQL (из connections в Airflow) к указанной БД
postgres_hook = PostgresHook(DB_CONNECTION)

# Создаем подключение к БД по полученным ранее настройкам
engine = postgres_hook.get_sqlalchemy_engine()


# -------
# Функции
# -------

# Функция чтения данных из файла и загрузки сырых данных в БД в схему stage (таблицы создаются автоматически, если их не было до этого)
def insert_data(table_name, encoding=None):

    # Читаем информацию из csv файла с таблицей table_name. Эти файлы лежат в папке Airflow/files
    # Параметры: путь файла, разделитель, кодировка (в стандартной utf8 один символ таблицы md_currency_d оставался загадочным)
    df = pandas.read_csv(f"{PATH}{table_name}.csv", delimiter=";", encoding=encoding)

    # Методом to_sql загружаем наш датафрейм в БД (создаем/обновляем таблицы и загружаем туда данные)
    # Параметры: название таблицы, подключение, схема, поведение при существовании такой таблицы в СУБД (append = Insert new values to the existing table), наличие индекса
    # df.to_sql(table_name, engine, schema="stage", if_exists="append", index=False)
    df.to_sql(table_name, engine, schema="stage", if_exists="replace", index=False)


# Функция для логирования в базу данных времени начала загрузки
def log_start(**kwargs):
    context = kwargs
    cur_run_id = context['dag_run'].run_id  # id текущего запуска dag, для идентификации записи в таблице логов
    with engine.connect() as connection:
        connection.execute(f"INSERT INTO logs.load_logs (event_id, start_time) VALUES ('{cur_run_id}', current_timestamp);")
        time.sleep(5)  # Задержка на 5 секунд, которая требуется по заданию


# Функция для логирования в базу данных времени начала загрузки
def log_end(**kwargs):
    context = kwargs
    cur_run_id = context['dag_run'].run_id
    with engine.connect() as connection:
        connection.execute(f"UPDATE logs.load_logs SET end_time = current_timestamp WHERE event_id = '{cur_run_id}';")


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
    description="Загрузка данных в stage",  # описание DAG
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

    # sql_log_start = SQLExecuteQueryOperator(
    #     task_id="sql_log_start",
    #     conn_id=DB_CONNECTION,
    #     sql=f"INSERT INTO logs.load_logs (event_id, start_time) VALUES ('{cur_run_id}', current_timestamp);"
    #     # sql=log_start
    # )

    # sql_log_end = SQLExecuteQueryOperator(
    #     task_id="sql_log_end",
    #     conn_id=DB_CONNECTION,
    #     # sql=f"UPDATE logs.load_logs SET end_time = current_timestamp WHERE event_id = '{cur_run_id}';"
    #     sql=f"INSERT INTO logs.load_logs (event_id, end_time) VALUES ('{cur_run_id}', current_timestamp);"
    #     # sql=log_end
    # )

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

    # # --------
    # # Вызов процедур, созданных заранее в БД (оператор тот же, что и для sql скриптов)

    # sql_get_posting_data_by_date = SQLExecuteQueryOperator(
    #     task_id="sql_get_posting_data_by_date",
    #     conn_id=DB_CONNECTION,
    #     sql="CALL dm.get_posting_data_by_date()"  # вместо пути до файла задаем запрос - вызов функции
    # )

    # # Вызов запроса для получения суммы проводок по дебету и кредиту за 15.01.2018

    # sql_get_debet_and_credit_sums = SQLExecuteQueryOperator(
    #     task_id="sql_get_debet_and_credit_sums",
    #     conn_id=DB_CONNECTION,
    #     sql="CALL dm.get_debet_and_credit_sums()"
    # )

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
