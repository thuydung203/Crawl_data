import requests
import csv
import re
import time
import html
import sys
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

# Force output to UTF-8
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# ================= CONFIG =================
BASE_URL = "https://gialai.gov.vn"
OUTPUT_FILE = "gialai_data_final.csv"
MAX_PAGES_PER_TOPIC = 50 
SLEEP = 0.5

CATEGORIES = {
    "Chỉ đạo điều hành": "https://gialai.gov.vn/tin-tuc/thong-tin-chi-dao-dieu-hanh",
    "Hoạt động lãnh đạo": "https://gialai.gov.vn/tin-tuc/hoat-dong-cua-lanh-dao",
    "Chính trị": "https://gialai.gov.vn/tin-tuc/tin-tuc-chinh-tri",
    "Kinh tế": "https://gialai.gov.vn/tin-tuc/tin-kinh-te-tong-hop",
    "Văn hóa - Xã hội": "https://gialai.gov.vn/tin-tuc/tin-van-hoa-xa-hoi",
    "Đối ngoại": "https://gialai.gov.vn/tin-tuc/thong-tin-doi-ngoai",
    "Sở ban ngành": "https://gialai.gov.vn/tin-tuc/tin-tu-so-ban-nganh",
    "Địa phương": "https://gialai.gov.vn/tin-tuc/tin-tu-thi-xa-huyen-thanh-pho"
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    # Remove control characters
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def normalize_url(url):
    if not url: 
        return None
    url = url.strip()
    if url.startswith("http"):
        return url
    return urljoin(BASE_URL, url)

def format_date(date_str):
    if not date_str:
        return ""
    # Look for dd/mm/yyyy
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", date_str)
    if match:
        return match.group(1)
    return date_str

# ================= PARSE ARTICLE =================
def parse_article(url, topic):
    result = {
        "topic": topic,
        "title": "",
        "summary": "",
        "url": url,
        "keywords": "",
        "public_time": "",
        "content": ""
    }
    
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # TITLE
        title_tag = soup.find("h1", class_="title-detail")
        if title_tag:
            result["title"] = clean_text(title_tag.get_text())
        
        # PUBLIC TIME
        time_tag = soup.find(class_="post-date")
        if time_tag:
            result["public_time"] = format_date(clean_text(time_tag.get_text()))
        
        # SUMMARY
        summary_div = soup.find(class_="article-brief") or soup.find(class_="common-brief")
        if summary_div:
            result["summary"] = clean_text(summary_div.get_text())

        # CONTENT
        content_div = soup.find(class_="article-content") or soup.find(class_="common-content")
            
        if content_div:
            # Deep Cleanup
            for tag in content_div.find_all([
                "script", "style", "iframe", "form", "nav", "header", "footer"
            ]):
                tag.decompose()
            
            # Remove social sharing, related news containers if they are inside content
            for tag in content_div.find_all(class_=re.compile("social|rating|comment|related|tags|keywords", re.I)):
                tag.decompose()
                
            paragraphs = []
            for p in content_div.find_all(["p", "div", "span"], recursive=False):
                txt = clean_text(p.get_text())
                
                # Skip image captions or source lines
                if any(x in txt.lower() for x in ["ảnh:", "ảnh minh họa", "nguồn:", "bài, ảnh:"]):
                    continue
                
                # Stop if we hit common end-of-article boilerplate
                if any(x in txt.lower() for x in ["tin liên quan", "ý kiến của bạn", "đánh giá bài viết", "bình luận"]):
                    break
                
                if len(txt) > 20: 
                    paragraphs.append(txt)
            
            if paragraphs:
                result["content"] = " ".join(paragraphs)
            else:
                # Fallback to general text if no paragraphs found
                result["content"] = clean_text(content_div.get_text())

        # KEYWORDS
        meta_kw = soup.find("meta", attrs={"name": "keywords"})
        if meta_kw:
            result["keywords"] = clean_text(meta_kw.get("content"))
        
        # If no content but title/summary exist, it might be a shell page or different structure
        if not result["content"] and not result["title"]:
            return None

        # Requirement: only save if has title and non-empty summary
        if not result["title"] or not result["summary"]:
            return None

        return result

    except Exception as e:
        print(f"Error parsing {url}: {e}")
        return None

# ================= PAGINATION & LINKS =================
def extract_article_links(soup, seen_urls):
    links = []
    # Selector identified from research: a.article-title.common-title
    for a in soup.select("a.article-title.common-title"):
        href = a.get("href")
        if href:
            full_url = normalize_url(href)
            if full_url and full_url not in seen_urls and BASE_URL in full_url:
                links.append(full_url)
    
    return list(dict.fromkeys(links))

def get_next_page_url(soup, current_page):
    """
    Finds the next page URL. 
    Gia Lai uses standard pagination with ?page=N or similar structure.
    Looking at standard .wrapper-pagination structure.
    """
    pagination = soup.find(class_="wrapper-pagination")
    if not pagination:
        return None
    
    next_link = pagination.find("a", class_="next-pages")
    if next_link and next_link.get("href"):
        return normalize_url(next_link["href"])
    
    # Fallback: look for a link with text containing current_page + 1
    next_page_num = current_page + 1
    for a in pagination.find_all("a"):
        if clean_text(a.get_text()) == str(next_page_num):
            return normalize_url(a.get("href"))
            
    return None

# ================= MAIN =================
def main():
    seen_urls = set()
    
    # Load existing URLs
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("url"):
                    seen_urls.add(row["url"])
        print(f"Loaded {len(seen_urls)} existing URLs.")
    except FileNotFoundError:
        pass

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["topic", "title", "summary", "url", "keywords", "public_time", "content"]
        )
        if len(seen_urls) == 0:
            writer.writeheader()

        for topic_name, start_url in CATEGORIES.items():
            print(f"\n=== Processing Topic: {topic_name} ===")
            
            current_url = start_url
            for page in range(1, MAX_PAGES_PER_TOPIC + 1):
                print(f"  -> Crawling Page {page}: {current_url}")
                
                try:
                    resp = session.get(current_url, timeout=30)
                    if resp.status_code != 200:
                        print(f"    Failed to load page {page}")
                        break
                        
                    soup = BeautifulSoup(resp.text, "html.parser")
                    links = extract_article_links(soup, seen_urls)
                    
                    if not links:
                        print("    No new links found.")
                        # Check if there's still a next page to be sure
                        next_url = get_next_page_url(soup, page)
                        if not next_url:
                            break
                        current_url = next_url
                        continue
                    
                    print(f"    Found {len(links)} new articles.")
                    
                    count_saved = 0
                    for link in links:
                        seen_urls.add(link)
                        data = parse_article(link, topic_name)
                        if data:
                            writer.writerow(data)
                            count_saved += 1
                        time.sleep(SLEEP)
                    
                    print(f"    Saved {count_saved}/{len(links)} articles.")
                    
                    # Get next page URL
                    next_url = get_next_page_url(soup, page)
                    if not next_url:
                        print("    No more pages.")
                        break
                    current_url = next_url
                    
                except Exception as e:
                    print(f"    Error on page {page}: {e}")
                    break
                    
                time.sleep(1)

if __name__ == "__main__":
    main()
