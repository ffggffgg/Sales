from extract import load_from_csv
from transform import transform_data
from load_and_visual import save_to_db, upload_to_s3

# 1. Збір даних
raw_data_csv = load_from_csv()

# 2. Трансформація
transformed_data = transform_data(raw_data_csv)

# 3. Завантаження
save_to_db(transformed_data, db_connection="postgresql://user:password@host/dbname")
