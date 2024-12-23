import requests
import pandas as pd
import psycopg2
from connect import load_config
import time


def extract(file_path):
    try:
        ids = pd.read_csv(file_path, usecols=[0]).squeeze().values
    except FileNotFoundError as err:
        print("File Not Found !!!")
    else:
        return ids

def transform(ids, api_url):
    products = []
    for id in ids:
        try:
            headers = {'User-Agent' : 'Mozilla/5.0'}
            response = requests.get(f'{api_url}/{id}', headers = headers)
            product_data = response.json()
            filter_product_data = {
                "id" : product_data.get("id"),
                "name": product_data.get("name"),
                "url_key": product_data.get("url_key"),
                "price": product_data.get("price"),
                "description": product_data.get("description"),
                "image_url": product_data.get("images")[0].get("base_url")
            }
            products.append(filter_product_data)
        except Exception as err:
            print(err)
            continue
    return products
def load(products):
    insert_query = "INSERT INTO products(id, name, url_key, price, description, image_url) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(id) DO NOTHING;"
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for product in products:
                    try:
                        cur.execute(insert_query, (product["id"], product["name"], product["url_key"], product["price"], product["description"], product["image_url"]))
                    except (Exception, psycopg2.DatabaseError) as err:
                        print(err)
                        continue
                conn.commit()
    except psycopg2.DatabaseError as err:
        print("Can't connect to database")