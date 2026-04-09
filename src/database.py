import sqlite3
from pathlib import Path
import pandas as pd


DATABASE_PATH = "data/quotes.db"

QUOTES_PATH = "data/processed/quotes/data.csv"
AUTHORS_PATH = "data/processed/authors/data.csv"
TAGS_PATH = "data/processed/tags/data.csv"
QUOTE_TAGS_PATH = "data/processed/quote_tags/data.csv"


def create_database():
    """
    Create SQLite database and tables
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS authors (
        author_id INTEGER PRIMARY KEY,
        author TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS quotes (
        quote_id INTEGER PRIMARY KEY,
        quote_text TEXT,
        author TEXT,
        page_number INTEGER,
        scraped_at TEXT,
        quote_length INTEGER,
        word_count INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tags (
        tag_id INTEGER PRIMARY KEY,
        tag TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS quote_tags (
        quote_id INTEGER,
        tag_id INTEGER
    )
    """)

    conn.commit()
    conn.close()


def load_data():
    """
    Load CSV files into database
    """
    conn = sqlite3.connect(DATABASE_PATH)

    quotes = pd.read_csv(QUOTES_PATH)
    authors = pd.read_csv(AUTHORS_PATH)
    tags = pd.read_csv(TAGS_PATH)
    quote_tags = pd.read_csv(QUOTE_TAGS_PATH)

    authors.to_sql("authors", conn, if_exists="replace", index=False)
    quotes.to_sql("quotes", conn, if_exists="replace", index=False)
    tags.to_sql("tags", conn, if_exists="replace", index=False)
    quote_tags.to_sql("quote_tags", conn, if_exists="replace", index=False)

    conn.close()


def main():
    print("Creating database...")
    create_database()

    print("Loading data...")
    load_data()

    print("Database created successfully!")


if __name__ == "__main__":
    main()