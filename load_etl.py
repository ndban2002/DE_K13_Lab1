from time import time
from etl import extract, transform, load
def load_etl(file_path, reverse = False, batch_size=1000):
    start_time = time.time()
    ids = extract(file_path)
    if reverse == True:
        ids = ids[::-1]
    for i in range(0, len(ids), batch_size):
    # for i in ids:
        products = transform(ids[i:i+batch_size], 'https://api.tiki.vn/product-detail/api/v1/products')
        load(products)
    total_time = time.time() - start_time
    print(f"Finished in {total_time} seconds")
if __name__ == '__main__':
    load_etl('products-0-200000.csv', reverse=True)