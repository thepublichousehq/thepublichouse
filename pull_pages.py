import csv
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md
import sqlite3
import os
from tqdm import tqdm
import concurrent.futures

def pull_page(url: str) -> tuple[str, str]:
    """Fetches the title and markdown-formatted content of a given URL.

    Args:
        url: The URL of the page to fetch.

    Returns:
        A tuple containing the page title and content. Returns empty strings if an error occurs.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string.strip() if soup.title and soup.title.string else ''
        content = md(str(soup.body)) if soup.body else ''
        return title, content
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return '', ''

def create_database(db_path: str) -> None:
    """Creates a SQLite database with the specified schema.

    Args:
        db_path: Path to the database file.
    """
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

def update_database(db_path: str) -> None:
    """Updates database records with missing titles and text content.

    Args:
        db_path: Path to the database file.
        batch_size: Number of URLs to process in each batch.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM data WHERE title = '' OR text = ''")
    total_rows = c.fetchone()[0]

    progress_bar = tqdm(total=total_rows, unit='rows', desc='Updating records')

    def process_url(url: str) -> tuple[str, str, str]:
        title, text = pull_page(url)
        return url, title, text

    batch_size = 100
    offset = 0
    while True:
        c.execute("SELECT url FROM data WHERE title = '' OR text = '' LIMIT ? OFFSET ?", (batch_size, offset))
        urls = [row[0] for row in c.fetchall()]
        
        if not urls:
            break  # No more URLs to process

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            for url, title, text in executor.map(process_url, urls):
                c.execute("UPDATE data SET title = ?, text = ? WHERE url = ?", (title, text, url))
                conn.commit()
                progress_bar.update(1)

        offset += batch_size

    progress_bar.close()
    conn.close()

if __name__ == "__main__":
    db_path = 'hackernews_stories.db'

    if not os.path.exists(db_path):
        print("Creating database...")
        create_database(db_path)
    else:
        print("Database already exists. Skipping creation.")

    print("Updating database...")
    update_database(db_path)
