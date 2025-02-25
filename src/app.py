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
    
    INSERT OR IGNORE INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES 
        (1, 'Lean Software Development: An Agile Toolkit', 240, 4.17, '9780320000000', '2003-05-18', 5),
        (2, 'Facing the Intelligence Explosion', 91, 3.87, NULL, '2013-02-01', 7),
        (3, 'Scala in Action', 419, 3.74, '9781940000000', '2013-04-10', 1),
        (4, 'Patterns of Software: Tales from the Software Community', 256, 3.84, '9780200000000', '1996-08-15', 1),
        (5, 'Anatomy Of LISP', 446, 4.43, '9780070000000', '1978-01-01', 3),
        (6, 'Computing machinery and intelligence', 24, 4.17, NULL, '2009-03-22', 4),
        (7, 'XML: Visual QuickStart Guide', 269, 3.66, '9780320000000', '2009-01-01', 5),
        (8, 'SQL Cookbook', 595, 3.95, '9780600000000', '2005-12-01', 7),
        (9, 'The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', 439, 4.29, '9781440000000', '2010-07-01', 6),
        (10, 'Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', 222, 3.54, '9780750000000', '2007-02-13', 7);
    
    INSERT OR IGNORE INTO book_authors (book_id, author_id) VALUES 
        (1, 1),
        (2, 8),
        (3, 7),
        (4, 6),
        (5, 5),
        (6, 4),
        (7, 3),
        (8, 2),
        (9, 4),
        (10, 1);         
   


    """)

# 4) Use Pandas to read and display a table
df_books = pd.read_sql("SELECT * FROM books;", conn)

print("\nLibros:")
print(df_books)

# 5) Close connection
conn.close()