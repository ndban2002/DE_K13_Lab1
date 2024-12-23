import psycopg2
from connect import load_config


def create_tables():
    """ Create tables in the PostgreSQL database"""
    command = """
        CREATE TABLE products (
            id BIGINT PRIMARY KEY,
            name VARCHAR(500) NOT NULL,
            url_key TEXT,
            price INTEGER NOT NULL,
            description TEXT,
            image_url TEXT
        );
        """
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                cur.execute(command)
                print("CREATED")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_tables()