import kagglehub
import os
import shutil
import requests
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 350)

def load_csv_data(custom_path):

    # Завантаження датасету
    path = kagglehub.dataset_download("rohitsahoo/sales-forecasting")
    print("\nPath to dataset files:", path)

    # Переміщення завантажених файлів
    for file_name in os.listdir(path):
        full_file_name = os.path.join(path, file_name)
        if os.path.isfile(full_file_name): shutil.move(full_file_name, custom_path)
    print("\nФайли успішно переміщені до:", custom_path)

    # Збереження DataFrame
    custom_path = custom_path + "\\train.csv"
    raw_data = pd.read_csv(custom_path, index_col="Row ID")
    return raw_data

def post_code_api(adress):
    # Підготовка параметрів для запиту
    import us
    adress = adress.reset_index(drop=True).to_dict(orient="records")
    s = us.states.lookup((adress[0]['State']))

    # Отриння поштового індексу через API запит
    response = requests.get(f"https://api.zippopotam.us/us/{s.abbr}/{adress[0]['City']}")
    return response.json()['places'][0]['post code']

