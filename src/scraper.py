import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://quotes.toscrape.com"
START_URL = f"{BASE_URL}/page/1/"
OUTPUT_FILE = Path("data/raw/quotes_raw.json")
LOG_FILE = Path("logs/pipeline.log")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}


def setup_logging() -> None:
    """Set up logging to file and console."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def fetch_page(url: str, retries: int = 3, delay: float = 1.5) -> str | None:
    """
    Fetch a web page with basic retry logic.

    Args:
        url: The URL to fetch.
        retries: Number of attempts before giving up.
        delay: Delay between retries in seconds.

    Returns:
        HTML text if successful, otherwise None.
    """
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Fetching: {url} (attempt {attempt})")
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            logging.error(f"Request failed for {url}: {exc}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Giving up on {url} after {retries} attempts.")
    return None


def parse_quotes(html: str, page_number: int) -> list[dict[str, Any]]:
    """
    Parse quote data from a page of HTML.

    Args:
        html: Raw HTML from the page.
        page_number: The current page number.

    Returns:
        A list of dictionaries, one per quote.
    """
    soup = BeautifulSoup(html, "html.parser")
    quote_blocks = soup.find_all("div", class_="quote")
    scraped_at = datetime.now().isoformat()

    quotes = []

    for block in quote_blocks:
        text_tag = block.find("span", class_="text")
        author_tag = block.find("small", class_="author")
        tag_elements = block.find_all("a", class_="tag")

        quote_data = {
            "quote_text": text_tag.get_text(strip=True) if text_tag else "",
            "author": author_tag.get_text(strip=True) if author_tag else "",
            "tags": [tag.get_text(strip=True) for tag in tag_elements],
            "page_number": page_number,
            "scraped_at": scraped_at
        }

        quotes.append(quote_data)

    logging.info(f"Parsed {len(quotes)} quotes from page {page_number}")
    return quotes


def find_next_page(html: str) -> str | None:
    """
    Find the next page link, if one exists.

    Args:
        html: Raw HTML from the current page.

    Returns:
        Full URL for the next page, or None if there is no next page.
    """
    soup = BeautifulSoup(html, "html.parser")
    next_button = soup.find("li", class_="next")

    if next_button:
        next_link = next_button.find("a")
        if next_link and next_link.get("href"):
            return f"{BASE_URL}{next_link['href']}"

    return None


def scrape_all_quotes(start_url: str = START_URL) -> list[dict[str, Any]]:
    """
    Scrape all quote pages starting from the first page.

    Args:
        start_url: The URL to begin scraping from.

    Returns:
        A complete list of scraped quote records.
    """
    all_quotes = []
    current_url = start_url
    page_number = 1

    while current_url:
        html = fetch_page(current_url)

        if html is None:
            logging.error(f"Skipping page {page_number} because it could not be fetched.")
            break

        page_quotes = parse_quotes(html, page_number)
        all_quotes.extend(page_quotes)

        current_url = find_next_page(html)
        page_number += 1

        if current_url:
            logging.info("Waiting 1.5 seconds before next request...")
            time.sleep(1.5)

    logging.info(f"Finished scraping. Total quotes collected: {len(all_quotes)}")
    return all_quotes


def save_to_json(data: list[dict[str, Any]], output_file: Path = OUTPUT_FILE) -> None:
    """
    Save scraped data to a JSON file.

    Args:
        data: List of quote records.
        output_file: Path to the output JSON file.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    logging.info(f"Saved scraped data to {output_file}")


def main() -> None:
    """Run the scraping process."""
    setup_logging()
    logging.info("Starting quote scraper...")

    quotes = scrape_all_quotes()
    save_to_json(quotes)

    logging.info("Scraping process completed successfully.")


if __name__ == "__main__":
    main()