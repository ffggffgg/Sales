import numpy as np
import pandas as pd
import boto3
import psycopg2
from extract import load_csv_data
from transform import transform_data
data = load_csv_data()
transformed_data = transform_data(data)

# Параметри підключення
# conn = psycopg2.connect(
#     dbname="your_database",
#     user="your_user",
#     password="your_password",
#     host="your_host",
#     port="5432"
# )
conn = psycopg2.connect(dbname="sales_data", user="postgres", password="7137", host="localhost", port="5433")
cursor = conn.cursor()

create_tables = """
CREATE TABLE IF NOT EXISTS segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    segment_id INT REFERENCES segments(segment_id)
);

CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    category_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS sub_categories (
    sub_category_id SERIAL PRIMARY KEY,
    sub_category_name TEXT UNIQUE NOT NULL,
    category_id INT REFERENCES categories(category_id)
);

CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    sub_category_id INT REFERENCES sub_categories(sub_category_id)
);

CREATE TABLE IF NOT EXISTS regions (
    region_id SERIAL PRIMARY KEY,
    region_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS locations (
    location_id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    postal_code FLOAT,
    region_id INT REFERENCES regions(region_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    order_date DATE NOT NULL,
    ship_date DATE NOT NULL,
    customer_id TEXT REFERENCES customers(customer_id),
    product_id TEXT REFERENCES products(product_id),
    location_id INT REFERENCES locations(location_id),
    sales NUMERIC NOT NULL
);
"""
cursor.execute(create_tables)
conn.commit()
print("Всі таблиці створені!")

def insert_data(tf_data):
    print(tf_data.head())
    segments = tf_data[['Segment']].drop_duplicates().reset_index(drop=True)
    segments['segment_id'] = range(1, len(segments) + 1)
    for _, row in segments.iterrows():
        print(row, '\n', _)
        cursor.execute("INSERT INTO segments (segment_id, segment_name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                       (row['segment_id'], row['Segment']))

    df_customers = tf_data[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates()
    df_customers = df_customers.merge(segments, how='left', on='Segment')[['Customer ID', 'Customer Name', 'segment_id']]
    for _, row in df_customers.iterrows():
        cursor.execute(
            "INSERT INTO customers (customer_id, customer_name, segment_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
            (row['Customer ID'], row['Customer Name'], row['segment_id']))

    df_categories = tf_data[['Category']].drop_duplicates().reset_index(drop=True)
    df_categories['category_id'] = range(1, len(df_categories) + 1)
    for _, row in df_categories.iterrows():
        cursor.execute(
            "INSERT INTO categories (category_id, category_name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (row['category_id'], row['Category']))

    df_sub_categories = tf_data[['Sub-Category', 'Category']].drop_duplicates()
    df_sub_categories = df_sub_categories.merge(df_categories, how='left', on='Category')[['Sub-Category', 'category_id']]
    df_sub_categories['sub_category_id'] = range(1, len(df_sub_categories) + 1)
    for _, row in df_sub_categories.iterrows():
        cursor.execute(
            "INSERT INTO sub_categories (sub_category_id, sub_category_name, category_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
            (row['sub_category_id'], row['Sub-Category'], row['category_id']))

    df_products = tf_data[['Product ID', 'Product Name', 'Sub-Category']].drop_duplicates()
    df_products = df_products.merge(df_sub_categories, how='left', on='Sub-Category')[['Product ID', 'Product Name', 'sub_category_id']]
    for _, row in df_products.iterrows():
        cursor.execute(
            "INSERT INTO products (product_id, product_name, sub_category_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
            (row['Product ID'], row['Product Name'], row['sub_category_id']))

    df_regions = tf_data[['Region']].drop_duplicates().reset_index(drop=True)
    df_regions['region_id'] = range(1, len(df_regions) + 1)
    for _, row in df_regions.iterrows():
        cursor.execute(
            "INSERT INTO regions (region_id, region_name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (row['region_id'], row['Region']))

    df_locations = tf_data[['City', 'State', 'Postal Code', 'Region']].drop_duplicates()
    df_locations = df_locations.merge(df_regions, how='left', on='Region')[['City', 'State', 'Postal Code', 'region_id']]
    df_locations['location_id'] = range(1, len(df_locations) + 1)
    for _, row in df_locations.iterrows():
        cursor.execute(
            "INSERT INTO locations (location_id, city, state, postal_code, region_id) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (row['location_id'], row['City'], row['State'], row['Postal Code'], row['region_id']))

    df_orders = tf_data[['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Product ID', 'City', 'State', 'Sales']]
    df_orders = df_orders.merge(df_locations,  how='left', on=['City', 'State'])[['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Product ID', 'location_id', 'Sales']]
    for _, row in df_orders.iterrows():
        cursor.execute(
            "INSERT INTO orders (order_id, order_date, ship_date, customer_id, product_id, location_id, sales) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (row['Order ID'], row['Order Date'], row['Ship Date'], row['Customer ID'], row['Product ID'],
             row['location_id'], row['Sales']))

    conn.commit()
    # Закриття підключення
    cursor.close()
    conn.close()

insert_data(transformed_data)