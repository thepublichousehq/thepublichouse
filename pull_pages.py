import csv
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md
import sqlite3
import os
from tqdm import tqdm

def pull_page(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string.strip() if soup.title and soup.title.string else ''
        content = md(str(soup.body)) if soup.body else ''
        return title, content
    except Exception as e:
        return '', ''  # Return empty strings if any exception occurs

def create_database(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS data (
                    url TEXT PRIMARY KEY,
                    title TEXT,
                    text TEXT,
                    website TEXT,
                    time TEXT,
                    label TEXT
                )''')
    conn.commit()
    conn.close()

def update_database(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM data WHERE title = '' OR text = ''")
    total_rows = c.fetchone()[0]

    progress_bar = tqdm(total=total_rows, unit='rows', desc='Updating records')

    c.execute("SELECT url FROM data WHERE title = '' OR text = ''")
    urls = (row[0] for row in c.fetchall())

    for url in urls:
        title, text = pull_page(url)
        c.execute("UPDATE data SET title = ?, text = ? WHERE url = ?", (title, text, url))
        conn.commit()
        progress_bar.update(1)
    
    progress_bar.close()
    conn.close()

if __name__ == "__main__":
    db_path = 'hackernews_stories.db'

    if not os.path.exists(db_path):
        print("Creating database...")
        create_database(db_path, input_csv)
    else:
        print("Database already exists. Skipping creation.")

    print("Updating database...")
    update_database(db_path)