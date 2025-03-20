import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()


def connect():
    global engine
    try:
        connection_string = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

engine = connect()

if engine is None:
    exit()


with engine.connect() as connection:
    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS publishers (
        publisher_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """))

    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS authors (
        author_id INT AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        middle_name VARCHAR(50) NULL,
        last_name VARCHAR(100) NULL
    )
    """))

    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS books (
        book_id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        total_pages INT NULL,
        rating DECIMAL(4, 2) NULL,
        isbn VARCHAR(13) NULL,
        published_date DATE,
        publisher_id INT NULL,
        CONSTRAINT fk_publisher FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id) ON DELETE SET NULL
    )
    """))

    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS book_authors (
        book_id INT NOT NULL,
        author_id INT NOT NULL,
        PRIMARY KEY (book_id, author_id),
        CONSTRAINT fk_book FOREIGN KEY (book_id) REFERENCES books(book_id) ON DELETE CASCADE,
        CONSTRAINT fk_author FOREIGN KEY (author_id) REFERENCES authors(author_id) ON DELETE CASCADE
    )
    """))


with engine.connect() as connection:
    connection.execute(text("""
    INSERT IGNORE INTO publishers (publisher_id, name) VALUES
        (1, 'O Reilly Media'),
        (2, 'A Book Apart'),
        (3, 'A K PETERS'),
        (4, 'Academic Press'),
        (5, 'Addison Wesley'),
        (6, 'Albert&Sweigart'),
        (7, 'Alfred A. Knopf')
    """))

    connection.execute(text("""
    INSERT IGNORE INTO authors (author_id, first_name, middle_name, last_name) VALUES
        (1, 'Merritt', NULL, 'Eric'),
        (2, 'Linda', NULL, 'Mui'),
        (3, 'Alecos', NULL, 'Papadatos'),
        (4, 'Anthony', NULL, 'Molinaro'),
        (5, 'David', NULL, 'Cronin'),
        (6, 'Richard', NULL, 'Blum'),
        (7, 'Yuval', 'Noah', 'Harari'),
        (8, 'Paul', NULL, 'Albitz')
    """))


df = pd.read_sql("SELECT * FROM publishers;", engine)
print(df)