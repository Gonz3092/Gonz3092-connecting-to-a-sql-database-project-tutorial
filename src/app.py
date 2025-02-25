import os
import pandas as pd
from dotenv import load_dotenv
import sqlite3

# Load environment variables
load_dotenv()

# 1) Connect to the database with SQLite
def connect():
    global engine
    try:
        db_name = "database.db"  # Nombre de la base de datos SQLite
        print("Iniciando conexión...")
        conn = sqlite3.connect(db_name)  # Crear conexión
        print("Conectado exitosamente!")
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
    
conn = connect()

if conn is None:
    exit() 

# 2) Create the tables
with conn:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS publishers (
        publisher_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS authors (
        author_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        middle_name TEXT NULL,
        last_name TEXT NULL
    );

    CREATE TABLE IF NOT EXISTS books (
        book_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        total_pages INTEGER NULL,
        rating REAL NULL,
        isbn TEXT NULL,
        published_date DATE,
        publisher_id INTEGER NULL,
        FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS book_authors (
        book_id INTEGER NOT NULL,
        author_id INTEGER NOT NULL,
        PRIMARY KEY (book_id, author_id),
        FOREIGN KEY (book_id) REFERENCES books(book_id) ON DELETE CASCADE,
        FOREIGN KEY (author_id) REFERENCES authors(author_id) ON DELETE CASCADE
    );
    """)

# 3) Insert data
with conn:
    conn.executescript("""
    INSERT OR IGNORE INTO publishers (publisher_id, name) VALUES
        (1, 'O Reilly Media'),
        (2, 'A Book Apart'),
        (3, 'A K PETERS'),
        (4, 'Academic Press'),
        (5, 'Addison Wesley'),
        (6, 'Albert&Sweigart'),
        (7, 'Alfred A. Knopf');

    INSERT OR IGNORE INTO authors (author_id, first_name, middle_name, last_name) VALUES
        (1, 'Merritt', NULL, 'Eric'),
        (2, 'Linda', NULL, 'Mui'),
        (3, 'Alecos', NULL, 'Papadatos'),
        (4, 'Anthony', NULL, 'Molinaro'),
        (5, 'David', NULL, 'Cronin'),
        (6, 'Richard', NULL, 'Blum'),
        (7, 'Yuval', 'Noah', 'Harari'),
        (8, 'Paul', NULL, 'Albitz');
    """)

# 4) Use Pandas to read and display a table
df = pd.read_sql("SELECT * FROM publishers;", conn)
print(df)

# 5) Close connection
conn.close()