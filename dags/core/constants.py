from airflow.providers.postgres.hooks.postgres import PostgresHook  # связь с БД PostgreSQL
from airflow.models import Variable  # импортируем переменные, которые мы вводили в настройках Airflow (http://localhost:8080/variable/)

# ----------
# Переменные и константы
# ----------

# Чтобы иметь возможность запускать sql скрипты из Airflow, зададим параметр template_searchpath - шаблон по которому ищется начальный путь до файлов.
# Вытаскиваем из настроек Airflow переменную my_path - путь до используемых файлов. Сейчас она имеет значение /files/
PATH = Variable.get("my_path")

# DB_CONNECTION = "postgres-ht"  # Связь с базой данных для дз "Практика"
DB_CONNECTION = "postgres-project"  # Связь с базой данных для проекта

# Получаем настройки подключения для PostgreSQL (из connections в Airflow) к указанной БД
postgres_hook = PostgresHook(DB_CONNECTION)

# Создаем подключение к БД по полученным ранее настройкам
engine = postgres_hook.get_sqlalchemy_engine()
