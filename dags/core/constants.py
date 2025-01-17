from airflow.providers.postgres.hooks.postgres import PostgresHook  # связь с БД PostgreSQL
from airflow.models import Variable  # импортируем переменные, которые мы вводили в настройках Airflow (http://localhost:8080/variable/)

# ----------
# Переменные и константы
# ----------

# Абсолютный путь до вашей папки с проектом. У меня это C:\Airflow (внутри лежат уже все файлы и папки из репозитория)
ABSOLUTE_AIRFLOW_PATH = "C:\\Airflow"

# Чтобы иметь возможность запускать sql скрипты из Airflow, зададим параметр template_searchpath - шаблон по которому ищется начальный путь до файлов.
# Вытаскиваем из настроек Airflow переменную my_path - путь до используемых файлов. Сейчас она имеет значение /files/
PATH = Variable.get("my_path")

# DB_CONNECTION = "postgres-ht"  # Связь с базой данных для дз "Практика"
# DB_CONNECTION = "postgres-project"  # Связь с базой данных для проектного задания 1
DB_CONNECTION = "postgres-project2"  # Связь с базой данных для проектного задания 2

# Получаем настройки подключения для PostgreSQL (из connections в Airflow) к указанной БД
postgres_hook = PostgresHook(DB_CONNECTION)

# Создаем подключение к БД по полученным ранее настройкам
engine = postgres_hook.get_sqlalchemy_engine()
