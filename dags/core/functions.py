import pandas  # библиотека для загрузки файлов из БД
import time

from .constants import PATH, engine


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
        connection.execute(f"""
            INSERT INTO logs.load_logs (run_id, start_time) 
            VALUES ('{cur_run_id}', current_timestamp);
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
