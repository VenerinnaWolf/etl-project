import pandas  # библиотека для загрузки файлов из БД
import time
from sqlalchemy import text

from .constants import ABSOLUTE_AIRFLOW_PATH, PATH, engine


# Функция чтения данных из файла и загрузки сырых данных в БД в схему stage (таблицы создаются автоматически, если их не было до этого)
def insert_data(table_name, delimiter=";", encoding=None):

    # Читаем информацию из csv файла с таблицей table_name. Эти файлы лежат в папке Airflow/files
    # Параметры: путь файла, разделитель, кодировка (в стандартной utf8 один символ таблицы md_currency_d оставался загадочным)
    df = pandas.read_csv(f"{PATH}{table_name}.csv", delimiter=delimiter, encoding=encoding)

    # Методом to_sql загружаем наш датафрейм в БД (создаем/обновляем таблицы и загружаем туда данные)
    # Параметры: название таблицы, подключение, схема, поведение при существовании такой таблицы в СУБД (append = Insert new values to the existing table), наличие индекса
    # df.to_sql(table_name, engine, schema="stage", if_exists="append", index=False)
    df.to_sql(table_name, engine, schema="stage", if_exists="replace", index=False)


# Та же функция, но с логированием
def insert_data(table_name, delimiter=";", encoding=None, **kwargs):
    task_name = f"insert_{table_name}"
    log_start(task_name, **kwargs)  # логируем начало

    # insert_data(table_name, delimiter, encoding)  # вызываем функцию, определенную выше
    df = pandas.read_csv(f"{PATH}{table_name}.csv", delimiter=delimiter, encoding=encoding)
    df.to_sql(table_name, engine, schema="stage", if_exists="replace", index=False)

    log_end(task_name, **kwargs)  # логируем конец


# Функция экспорта таблицы из базы данных в csv файл
def export_data(schema, table_name, **kwargs):
    task_name = f"export_{table_name}"
    log_start(task_name, **kwargs)  # логируем начало

    # file_path = f"{PATH}{table_name}.csv"  # Такой путь не работает, хотя он работает в функции pandas.read_csv при обратном импорте файлов в БД. Магия
    file_path = f"{ABSOLUTE_AIRFLOW_PATH}\\files\\{table_name}.csv"  # Тут должен быть абсолютный путь до ваших файлов
    open(file_path, 'w').close()  # Создаем файл, если его не существовало до этого. Если файл существовал, то очищаем его
    with engine.connect() as connection:
        connection.execute(f"""
            COPY {schema}.{table_name} TO '{file_path}' WITH (FORMAT CSV, HEADER);
        """)

    log_end(task_name, **kwargs)  # логируем конец


# Функция вызова sql скрипта из файла
def transform_data(table_name, **kwargs):
    task_name = f"transform_{table_name}"
    log_start(task_name, **kwargs)  # логируем начало

    file_path = f"{PATH}/sql/{table_name}.sql"  # А тут абсолютный путь не требуется :[
    with engine.connect() as connection:
        with open(file_path) as file:
            query = text(file.read())
            connection.execute(query)

    log_end(task_name, **kwargs)  # логируем конец


# Функция для логирования в базу данных времени начала загрузки
def log_start(**kwargs):
    context = kwargs
    cur_run_id = context['dag_run'].run_id  # id текущего запуска dag, для идентификации записи в таблице логов
    with engine.connect() as connection:
        connection.execute(f"""
            INSERT INTO logs.load_logs (run_id, start_time) 
            VALUES ('{cur_run_id}', current_timestamp);
        """)
        time.sleep(5)  # Задержка на 5 секунд, которая требуется по заданию


# Та же функция, только для логирования каждой задачи по отдельности
def log_start(task_name, **kwargs):
    context = kwargs
    cur_run_id = context['dag_run'].run_id  # id текущего запуска dag, для идентификации записи в таблице логов
    with engine.connect() as connection:
        connection.execute(f"""
            INSERT INTO logs.load_logs (run_id, task_name, start_time) 
                VALUES ('{cur_run_id}', '{task_name}', current_timestamp)
            ON CONFLICT ON CONSTRAINT load_logs_pkey DO UPDATE
                SET start_time = excluded.start_time;
        """)
        time.sleep(5)  # Задержка на 5 секунд, которая требуется по заданию


# Функция для логирования в базу данных времени начала загрузки
def log_end(**kwargs):
    context = kwargs
    cur_run_id = context['dag_run'].run_id
    with engine.connect() as connection:
        connection.execute(f"""
            UPDATE logs.load_logs 
            SET end_time = current_timestamp, 
                duration = current_timestamp - start_time 
            WHERE run_id = '{cur_run_id}';
        """)


# Та же функция, только для логирования каждой задачи по отдельности
def log_end(task_name, **kwargs):
    context = kwargs
    cur_run_id = context['dag_run'].run_id
    with engine.connect() as connection:
        connection.execute(f"""
            UPDATE logs.load_logs 
            SET end_time = current_timestamp, 
                duration = current_timestamp - start_time 
            WHERE run_id = '{cur_run_id}' AND task_name = '{task_name}';
        """)
