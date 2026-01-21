import requests
import re
from bs4 import BeautifulSoup
import csv
import os
import time
from urllib.parse import urljoin

# Configuration
BASE_URL = "https://hatinh.gov.vn"
OUTPUT_FILE = "hatinh_data_final.csv"

# Category URLs and their corresponding topics
CATEGORIES = {
    # "Tin trong tỉnh": "https://hatinh.gov.vn/vi/chuyen-muc/tin-trong-tinh",
    # "Tin trong nước": "https://hatinh.gov.vn/vi/chuyen-muc/tin-trong-nuoc",
    # "Hoạt động Sở - Ban - Ngành - Địa phương": "https://hatinh.gov.vn/vi/chuyen-muc/hoat-dong-so-ban-nganh-dia-phuong",
    # "Tin quốc tế": "https://hatinh.gov.vn/vi/chuyen-muc/tin-quoc-te",
    # "Số liệu kinh tế - xã hội": "https://hatinh.gov.vn/vi/chuyen-muc/so-lieu-kinh-te---xa-hoi",
    # "Đầu tư - Phát triển": "https://hatinh.gov.vn/vi/chuyen-muc/dau-tu-phat-trien",
    # "Doanh nghiệp": "https://hatinh.gov.vn/vi/chuyen-muc/doanh-nghiep",
    "Công dân": "https://hatinh.gov.vn/vi/chuyen-muc/cong-dan"
}

# CSV Headers
HEADERS = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]

def get_soup(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def extract_article_data(url, topic):
    soup = get_soup(url)
    if not soup:
        return None

    try:
        # Title
        title_tag = soup.select_one(".detail-title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        title = re.sub(r'\s+', ' ', title).strip()

        # Public Time
        time_tag = soup.select_one(".time")
        public_time = time_tag.get_text(strip=True).replace("Đăng tải:", "").strip() if time_tag else ""

        # Summary (Lead)
        summary_tag = soup.select_one(".detail-content-lead")
        summary = summary_tag.get_text(strip=True) if summary_tag else ""
        summary = re.sub(r'\s+', ' ', summary).strip()

        # Content
        content_tag = soup.select_one(".detail-content")
        # Remove scripts and styles from content
        if content_tag:
            for script in content_tag(["script", "style"]):
                script.decompose()
            content = content_tag.get_text(separator=" ", strip=True).replace("\n", " ")
            
            # Remove attachment and view count metadata
            # Pattern 1: Attachment info (more robust)
            # Removes "Tệp đính kèm:" followed by anything until "Lượt xem" or end of string/paragraph
            content = re.sub(r'Tệp đính kèm:.*?(?=Lượt xem:|Tác giả:|$)', '', content, flags=re.IGNORECASE | re.DOTALL)
            
            # Pattern 2: View count
            content = re.sub(r'Lượt xem:\s*\d+', '', content, flags=re.IGNORECASE)
            
            # Pattern 3: Author info
            content = re.sub(r'Tác giả:.*?(?=$)', '', content, flags=re.IGNORECASE)

            # Pattern 5: Captions explicitly saying "Ảnh minh họa"
            content = re.sub(r'\(?Ảnh minh ho[ạa].*?\)?\.?', '', content, flags=re.IGNORECASE)

            # Pattern 4: Photo captions
            content = re.sub(r'Ảnh:.*?(?=\n|\.|$)', '', content, flags=re.IGNORECASE)
            
            # General cleanup of leftover artifacts
            content = re.sub(r'\s+\.\s+', '. ', content) # Fix stuck periods like " . "

            # Remove multiple spaces
            content = " ".join(content.split())
        else:
            content = ""

        # Keywords (Try meta tag)
        keywords = ""
        meta_keywords = soup.find("meta", attrs={"name": "keywords"})
        if meta_keywords:
            keywords = meta_keywords.get("content", "")

        return {
            "topic": topic,
            "title": title,
            "summary": summary,
            "url": url,
            "keywords": keywords,
            "public_time": public_time,
            "content": content
        }

    except Exception as e:
        print(f"Error extracting data from {url}: {e}")
        return None

def get_existing_urls(file_path):
    urls = set()
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'url' in row and row['url']:
                    urls.add(row['url'].strip())
    return urls

def crawl():
    # Initialize CSV file
    existing_urls = set()
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS)
            writer.writeheader()
    else:
        existing_urls = get_existing_urls(OUTPUT_FILE)
        print(f"Loaded {len(existing_urls)} existing URLs.")

    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        
        for topic, cat_url in CATEGORIES.items():
            print(f"Crawling category: {topic} - {cat_url}")
            print(f"Crawling category: {topic} - {cat_url}")
            page = 1
            while True:
                page_url = f"{cat_url}?page={page}"
                print(f"  Fetching page {page}: {page_url}")
                
                soup = get_soup(page_url)
                if not soup:
                    break

                # Find article links
                # Adjust selector based on actual list structure. 
                # Usually .news-item a or similar. Inspecting...
                # Based on analysis, let's look for common patterns in news lists
                # Often in .list-news or similar containers.
                # Let's try a generic approach if specific list class wasn't in analysis
                # But wait, I need to know the list item selector. 
                # I'll try to find links that look like article links.
                
                # Analyzing list page structure safely:
                # Usually links are in h3 or h2 titles.
                article_links = []
                # Try finding links within a likely news container
                # Often 'item-news', 'news-item', 'post-item'
                
                # Let's try getting all links in the main content area, avoiding specific classes if unsure,
                # but filtering by having 'bai-viet' in URL might be a safe bet for this site if it follows that pattern
                # The analyzed article URL was: https://hatinh.gov.vn/bai-viet/...
                
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if '/bai-viet/' in href:
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in article_links:
                            article_links.append(full_url)
                
                # Deduplicate and filter
                article_links = list(set(article_links))
                
                if not article_links:
                    print(f"  No articles found on page {page}. Stopping category.")
                    break
                
                print(f"  Found {len(article_links)} articles.")
                
                consecutive_duplicates = 0
                max_consecutive_duplicates = 50 
                
                for article_url in article_links:
                    if article_url in existing_urls:
                        print(f"    Skipping (already crawled): {article_url}")
                        consecutive_duplicates += 1
                        if consecutive_duplicates >= max_consecutive_duplicates:
                             print(f"  Reached max consecutive duplicates ({max_consecutive_duplicates}). Skipping to next topic.")
                             break
                        continue
                        
                    consecutive_duplicates = 0 # Reset counter if new article found
                    print(f"    Processing: {article_url}")
                    data = extract_article_data(article_url, topic)
                    if data:
                        writer.writerow(data)
                        f.flush() # Ensure data is written immediately
                        existing_urls.add(article_url) # Add to set to avoid dups in same run
                        print(f"    Saved: {article_url}")
                    else:
                        print(f"    Failed to extract data: {article_url}")
                    time.sleep(0.1) # Reduce delay to speed up

                if consecutive_duplicates >= max_consecutive_duplicates:
                    break

                page += 1
                time.sleep(1)

if __name__ == "__main__":
    crawl()
