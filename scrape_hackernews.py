import requests
from datetime import datetime
import pytz
from typing import Dict, Any
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import os

def load_last_processed_id():
    if os.path.exists('last_processed_id.txt'):
        with open('last_processed_id.txt', 'r') as f:
            return int(f.read().strip())
    return None

def save_last_processed_id(last_id):
    with open('last_processed_id.txt', 'w') as f:
        f.write(str(last_id))

def fetch_and_process_urls(urls):
    def fetch_url(url):
        try:
            response = requests.get(url)
            item = response.json()
            if item and item.get("type") == "story" and item.get("url"):
                story_data = {
                    "url": item.get("url"),
                    "title": item.get("title", ""),
                    "text": item.get("text", ""),
                    "website": item.get("url").split("//")[1].split("/")[0],
                    "time": item.get("time"),
                    "label": "",
                }
                return story_data
        except Exception as e:
            print(f"Error fetching {url}: {str(e)}")
        return None

    with ThreadPoolExecutor(max_workers=2) as executor:  # Reduced number of workers
        futures = {executor.submit(fetch_url, url): url for url in urls}
        for future in tqdm(as_completed(futures), total=len(urls), desc="Fetching URLs"):
            result = future.result()
            if result:
                yield result

def scrape_hackernews():
    print("Starting Hacker News scraping")

    max_id = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()
    last_processed_id = load_last_processed_id()

    if last_processed_id:
        start_id = last_processed_id - 1
        print(f"Resuming from ID: {start_id}")
    else:
        start_id = max_id - 1

    csv_file = "hackernews_stories.csv"
    fieldnames = ["url", "title", "text", "website", "time", "label"]

    mode = 'a' if last_processed_id else 'w'
    with open(csv_file, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()

        chunk_size = 1000  # Adjust as needed
        current_id = start_id
        chunk_count = 0
        total_chunks_estimate = (start_id - 1) // chunk_size + 1

        while current_id > 1:
            chunk_ids = range(current_id, max(1, current_id - chunk_size), -1)
            urls = [f"https://hacker-news.firebaseio.com/v0/item/{id}.json" for id in chunk_ids]

            # Use generator to fetch and write stories one by one
            for story in fetch_and_process_urls(urls):
                writer.writerow(story)
                f.flush()  # Ensure data is written to disk

            # Save progress after each chunk
            last_processed_id = chunk_ids[0]
            save_last_processed_id(last_processed_id)

            chunk_count += 1
            print(f"Finished processing chunk {chunk_count}/{total_chunks_estimate}")
            print(f"Last processed ID: {last_processed_id}")

            # Prepare for the next chunk
            current_id -= chunk_size

            # Add a small delay between chunks to allow for I/O operations
            time.sleep(0.1)

    print(f"All stories saved to {csv_file}")

if __name__ == "__main__":
    scrape_hackernews()