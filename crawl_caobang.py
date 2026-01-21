import requests
import csv
import re
import time
import html
import os
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_URL = "https://caobang.gov.vn"
CATEGORIES = {
    "Chính trị": "https://caobang.gov.vn/chinh-tri",
    "Kinh tế": "https://caobang.gov.vn/kinh-te",
    "Văn hóa - Xã hội": "https://caobang.gov.vn/van-hoaxa-hoi",
    "Quốc phòng - An ninh": "https://caobang.gov.vn/quoc-phong-an-ninh"
}

OUTPUT_FILE = "caobang_data_final.csv"
MAX_PAGES_PER_CATEGORY = 100
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def clean_text(s):
    if not s:
        return ""
    s = html.unescape(s)
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def get_soup(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return BeautifulSoup(response.text, "html.parser")
        return None
    except Exception as e:
        print(f"Error accessing {url}: {e}")
        return None

def extract_article_details(article_url, topic):
    soup = get_soup(article_url)
    if not soup:
        return None

    # --- Title ---
    title = ""
    title_candidates = [
        soup.select_one(".ArticleHeader"),
        soup.select_one(".title-detail"),
        soup.find("h1")
    ]
    for candidate in title_candidates:
        if candidate:
            title = clean_text(candidate.get_text())
            break
            
    # --- Public Time ---
    public_time = ""
    date_candidates = [
        soup.select_one(".PostDate"),
        soup.select_one(".date"),
        soup.select_one(".datetime")
    ]
    for candidate in date_candidates:
        if candidate:
            public_time = clean_text(candidate.get_text())
            break
            
    # --- Content ---
    content = ""
    content_div = soup.select_one(".ArticleContent") or soup.select_one(".newsbody")
    if content_div:
        # Cleanup
        for tag in content_div(["script", "style", "iframe", "form", "div"]): 
            # Be careful removing all divs, but often safer for text extraction
            # If divs contain real content, might want to keep, but usually p tags are enough
             if tag.name == "div" and not tag.find("p"): 
                 pass # keep container divs if needed? 
             elif tag.name != "div":
                 tag.decompose()
                 
        paragraphs = [clean_text(p.get_text()) for p in content_div.find_all("p")]
        content = " ".join([p for p in paragraphs if p])
    
    # --- Summary ---
    summary = ""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        summary = clean_text(meta_desc.get("content"))
    
    # Fallback summary: first paragraph of content
    if not summary and content:
        summary = content.split(".")[0] + "."

    # --- Keywords ---
    keywords = ""
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw:
        keywords = clean_text(meta_kw.get("content"))

    return {
        "topic": topic,
        "title": title,
        "summary": summary,
        "url": article_url,
        "keywords": keywords,
        "public_time": public_time,
        "content": content
    }

def crawl_category(category_name, category_url, writer):
    print(f"Crawling category: {category_name}...")
    
    seen_urls = set() # Per category session to avoid dupes

    for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
        if page == 1:
            page_url = category_url
        else:
            page_url = f"{category_url}/page/{page}"
        
        print(f"  Scanning page {page}: {page_url}")
        soup = get_soup(page_url)
        if not soup:
            break
        
        # Find article links
        # Heuristic: links that look like articles (contain dash-number)
        links = []
        main_content = soup.select_one(".content-right") or soup.select_one(".ModuleContent") or soup.body
        
        for a in main_content.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/") or href.startswith(BASE_URL):
                if re.search(r'-\d+$', href) and "page/" not in href:
                    full_url = href if href.startswith("http") else BASE_URL + href
                    if full_url not in seen_urls:
                        links.append(full_url)
                        seen_urls.add(full_url)
        
        if not links:
            print("    No new articles found. Stopping.")
            break
            
        print(f"    Found {len(links)} new articles.")
        
        for link in links:
            try:
                details = extract_article_details(link, category_name)
                if details and details["title"] and details["content"]:
                    writer.writerow(details)
                    print(f"      Saved: {details['title'][:50]}...")
                # time.sleep(0.5) # Polite delay
            except Exception as e:
                print(f"      Error: {e}")

def main():
    # Helper to clean text
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    with open(OUTPUT_FILE, mode="w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for category, url in CATEGORIES.items():
            crawl_category(category, url, writer)

if __name__ == "__main__":
    main()
