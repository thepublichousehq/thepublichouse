import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md
import sqlite3
import os
from tqdm import tqdm

def pull_page(url: str) -> tuple[str, str]:
    """Fetches the title and markdown-formatted content of a given URL.

    Args:
        url: The URL of the page to fetch.

    Returns:
        A tuple containing the page title and content. Returns 'Error' if an error occurs.
    """
    try:
        response = requests.get(url, timeout=15)  # Updated timeout to 15 seconds
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string.strip() if soup.title and soup.title.string else ''
        content = md(str(soup.body)) if soup.body else ''
        return title, content
    except Exception as e:
        return 'Error', 'Error'

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
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM data WHERE title = '' OR text = ''")
    total_rows = c.fetchone()[0]

    progress_bar = tqdm(total=total_rows, unit='rows', desc='Updating records')

    def process_url(url: str) -> tuple[str, str, str]:
        title, text = pull_page(url)
        return url, title, text

    batch_size = 10  # Reduced batch size
    while True:
        c.execute("SELECT url FROM data WHERE title = '' OR text = '' LIMIT ?", (batch_size,))
        urls = [row[0] for row in c.fetchall()]
        
        if not urls:
            break  # No more URLs to process

        # Process URLs sequentially
        results = [process_url(url) for url in urls]

        # Begin transaction
        c.execute('BEGIN TRANSACTION')
        for url, title, text in results:
            c.execute("UPDATE data SET title = ?, text = ? WHERE url = ?", (title, text, url))
            progress_bar.update(1)
        # Commit transaction
        conn.commit()
    
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