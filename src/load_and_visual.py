import pandas as pd
import psycopg2
from extract import load_csv_data
from transform import transform_data

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
    def insert_table(table_name, data, columns, conflict_column=None):
        placeholders = ', '.join(['%s'] * len(columns))
        cols_str = ', '.join(columns)
        conflict_str = f" ON CONFLICT ({conflict_column}) DO NOTHING" if conflict_column else " ON CONFLICT DO NOTHING"
        query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}){conflict_str};"
        cursor.executemany(query, data)

    # SEGMENTS
    segments = tf_data[['Segment']].drop_duplicates().reset_index(drop=True)
    segments['segment_id'] = range(1, len(segments) + 1)
    data_segments = list(segments[['segment_id', 'Segment']].itertuples(index=False, name=None))
    insert_table('segments', data_segments, ['segment_id', 'segment_name'], conflict_column='segment_id')

    # CUSTOMERS
    df_customers = tf_data[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates()
    df_customers = df_customers.merge(segments, how='left', on='Segment')
    data_customers = list(df_customers[['Customer ID', 'Customer Name', 'segment_id']].itertuples(index=False, name=None))
    insert_table('customers', data_customers, ['customer_id', 'customer_name',
                                               'segment_id'], conflict_column='customer_id')

    # CATEGORIES
    df_categories = tf_data[['Category']].drop_duplicates().reset_index(drop=True)
    df_categories['category_id'] = range(1, len(df_categories) + 1)
    list_categories = list(df_categories[['category_id', 'Category']].itertuples(index=False, name=None))
    insert_table('categories', list_categories, ['category_id',
                                                 'category_name'], conflict_column='category_id')

    # SUB_CATEGORIES
    df_sub_categories = tf_data[['Sub-Category', 'Category']].drop_duplicates()
    df_sub_categories = df_sub_categories.merge(df_categories, how='left', on='Category')[['Sub-Category', 'category_id']]
    df_sub_categories['sub_category_id'] = range(1, len(df_sub_categories) + 1)
    list_sub_categories = list(df_sub_categories[['sub_category_id', 'Sub-Category',
                                                                    'category_id']].itertuples(index=False, name=None))
    insert_table('sub_categories', list_sub_categories, ['sub_category_id',
                                            'sub_category_name', 'category_id'], conflict_column='sub_category_id')

    # PRODUCTS
    df_products = tf_data[['Product ID', 'Product Name', 'Sub-Category']].drop_duplicates()
    df_products = df_products.merge(df_sub_categories, how='left', on='Sub-Category')[['Product ID', 'Product Name',
                                                                                                'sub_category_id']]
    df_products = list(df_products[['Product ID', 'Product Name', 'sub_category_id']].itertuples(index=False, name=None))
    insert_table('products', df_products, ['product_id', 'product_name',
                                           'sub_category_id'], conflict_column='product_id')

    # REGIONS
    df_regions = tf_data[['Region']].drop_duplicates().reset_index(drop=True)
    df_regions['region_id'] = range(1, len(df_regions) + 1)
    list_regions = list(df_regions[['region_id', 'Region']].itertuples(index=False, name=None))
    insert_table('regions', list_regions, ['region_id', 'region_name'], conflict_column='region_id')

    # LOCATIONS
    df_locations = tf_data[['City', 'State', 'Postal Code', 'Region']].drop_duplicates()
    df_locations = df_locations.merge(df_regions, how='left', on='Region')[['City', 'State', 'Postal Code', 'region_id']]
    df_locations['location_id'] = range(1, len(df_locations) + 1)
    list_locations = list(df_locations[['location_id', 'City', 'State', 'Postal Code',
                                                                    'region_id']].itertuples(index=False, name=None))
    insert_table('locations', list_locations, ['location_id', 'city', 'state', 'postal_code',
                                                                    'region_id'], conflict_column='location_id')

    # ORDERS
    df_orders = tf_data[['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Product ID', 'City', 'State', 'Sales']]
    df_orders = df_orders.merge(df_locations,  how='left', on=['City', 'State'])[['Order ID', 'Order Date', 'Ship Date',
                                                                'Customer ID', 'Product ID', 'location_id', 'Sales']]
    df_orders = list(df_orders[['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Product ID', 'location_id',
                                                                    'Sales']].itertuples(index=False, name=None))
    insert_table('orders', df_orders, ['order_id', 'order_date', 'ship_date', 'customer_id',
                                        'product_id', 'location_id', 'sales'], conflict_column='order_id')

    # Закриваємо під кінець
    conn.commit()
    cursor.close()
    conn.close()
