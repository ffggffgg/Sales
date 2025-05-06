import pandas as pd
from extract import post_code_api

def transform_data(csv_data):

    # Перевірка пустих значень
    print(f'\nNaNs:\n{csv_data.isnull().sum()}')
    csv_data = solve_nan(csv_data)
    print(f'\nNaNs Postal Code = {csv_data["Postal Code"].isnull().sum()}')

    # Огляд та корекція типів даних
    print('\nOverview Types:\n', csv_data.dtypes)
    csv_data = optimize_types(csv_data)
    print('\nTypes changed')

    # Перевірка повторюваних значень
    print(f'\nDuplicates = {csv_data.duplicated().sum()}', csv_data[csv_data.duplicated(keep = False) == True])
    csv_data = csv_data.drop_duplicates() # Видалення дублікатів
    print(f'\nDuplicates = {csv_data.duplicated().sum()}')

    # Детальний розгляд
    print('\nDescribe:\n', csv_data.describe(include='all'))

    return csv_data

def solve_nan(csv_data):
    address = csv_data[csv_data['Postal Code'].isnull()][['State', 'City']].drop_duplicates() # Виокремлення локацій з відсутнім індексом
    csv_data['Postal Code'] = csv_data['Postal Code'].fillna(float(post_code_api(address))) # Виклик функції запиту з присвоенням відповіді
    return csv_data

def optimize_types(csv_data):
    csv_data['Order Date'] = pd.to_datetime(csv_data['Order Date'], format='%d/%m/%Y', errors='coerce').dt.date # Перетворення типу дат
    csv_data['Ship Date'] = pd.to_datetime(csv_data['Ship Date'], format='%d/%m/%Y', errors='coerce').dt.date
    csv_data['Postal Code'] = csv_data['Postal Code'].astype(int) # Перетворення типу поштових індексів
    for col in csv_data.select_dtypes(include = ['object']):
        # Перетворення в категорії, якщо мало унікальних значень
        if csv_data[col].nunique() < 50:
            csv_data[col] = csv_data[col].astype('category')
    return csv_data





