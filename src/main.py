from extract import load_csv_data
from transform import transform_data
from load_and_visual import save_to_db

# Вкажіть шлях до директорії, куди потрібно зберігати дані
custom_path = "C:\\Users\\yaski\\WorkProjects\\Projects\\Sales\\data"  # Замініть на свій шлях

# 1. Збір даних
raw_data_csv = load_csv_data(custom_path)

# 2. Трансформація
transformed_data = transform_data(raw_data_csv)

# Параметри підключення
conn = psycopg2.connect(
    dbname="your_database",
    user="your_user",
    password="your_password",
    host="your_host",
    port="5432"
)

# 3. Завантаження
insert_data(transformed_data, db_connection="postgresql://user:password@host/" + conn)
