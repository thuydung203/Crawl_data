import requests
import csv
import re
import time
import html
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sys

# Force output to UTF-8
sys.stdout.reconfigure(encoding='utf-8')

# ================= CONFIG =================
BASE_URL = "https://www.cantho.gov.vn"
OUTPUT_FILE = "cantho_data_final.csv"
MAX_PAGES_PER_TOPIC = 50 # Adjust as needed
SLEEP = 0.5

CATEGORIES = {
    "Hoạt động Lãnh đạo thành phố": "https://www.cantho.gov.vn/hoat-dong-lanh-dao-thanh-pho",
    "Chỉ đạo điều hành Trung ương - Địa phương": "https://www.cantho.gov.vn/chi-dao-dieu-hanh-trung-uong-dia-phuong",
    "Tuyên truyền - Phổ biến pháp luật": "https://www.cantho.gov.vn/tuyen-truyen-pho-bien-phap-luat",
    "Hướng tới đại hội đại biểu toàn quốc lần thứ XIV của Đảng": "https://www.cantho.gov.vn/huong-toi-dai-hoi-dai-bieu-toan-quoc-lan-thu-xiv-cua-dang",
    "Bầu cử Đại biểu Quốc hội và Hội đồng nhân dân": "https://www.cantho.gov.vn/bau-cu-dai-bieu-quoc-hoi-va-hoi-dong-nhan-dan",
    "Công khai thông tin": "https://www.cantho.gov.vn/cong-khai-thong-tin",
    "Tin tức - Sự kiện nổi bật": "https://www.cantho.gov.vn/tin-tuc-va-su-kien",
    "Thông tin cần biết": "https://www.cantho.gov.vn/thong-tin-can-biet"
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

def clean_text(s):
    if not s:
        return ""
    s = html.unescape(s)
    # Normalize whitespace FIRST to turn newlines/tabs into spaces
    s = re.sub(r"\s+", " ", s)
    # Then strip other control characters
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return s.strip()

def normalize_url(url):
    if not url: return None
    url = url.strip()
    if url.startswith("http"): return url
    return urljoin(BASE_URL, url)

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
        title_tag = soup.find(class_="ArticleHeader") or \
                    soup.find(class_="PostTitle") or \
                    soup.find("h1")
        if title_tag:
            result["title"] = clean_text(title_tag.get_text())

        # PUBLIC TIME
        time_tag = soup.find(class_="PostDate") or \
                   soup.find(class_="ArticleDate") or \
                   soup.find(class_="date")
        if time_tag:
            result["public_time"] = clean_text(time_tag.get_text())
            
        # SUMMARY
        # Try meta description first
        meta_desc = soup.find("meta", attrs={"name": "description"}) or \
                    soup.find("meta", property="og:description")
        if meta_desc:
            result["summary"] = clean_text(meta_desc.get("content"))

        # Fallback summary if meta_desc is empty or too short
        if not result["summary"] or len(result["summary"]) < 50:
            summary_tag = soup.find(class_="ArticleSummary") or soup.find(class_="summary")
            if summary_tag:
                result["summary"] = clean_text(summary_tag.get_text(separator=" "))
            
        # KEYWORDS
        meta_kw = soup.find("meta", attrs={"name": "keywords"}) or \
                  soup.find("meta", attrs={"name": "news_keywords"})
        if meta_kw:
            result["keywords"] = clean_text(meta_kw.get("content"))
        
        # Fallback to visible tags
        if not result["keywords"]:
            tags = soup.select(".tag a, .tags a, .keyword a, .keywords a, .news-tag a, #tag a")
            if tags:
                result["keywords"] = ", ".join([clean_text(t.get_text()) for t in tags])

        # CONTENT
        content_div = soup.find(class_="ArticleContent") or \
                      soup.find(class_="PostContent") or \
                      soup.find(class_="ArticleBody") or \
                      soup.find(id="content")
                      
        if content_div:
            # Remove junk
            for tag in content_div.find_all(["script", "style", "iframe", "div"], class_=re.compile("relate|comment|share")):
                tag.decompose()
            
            # Check for Legal Document indicators
            text_content_check = content_div.get_text(separator=" ")
            if "Số ký hiệu" in text_content_check and "Ngày ban hành" in text_content_check:
                return None

            # Extract all text units to maintain paragraph breaks and avoid swallowed words
            # We use separator=" " in get_text() for the whole block
            result["content"] = clean_text(content_div.get_text(separator=" "))

            # If summary is still missing or too short/truncated, try to get a better one from content
            if not result["summary"] or result["summary"].endswith("...") or len(result["summary"]) < 100:
                content_snippet = result["content"][:500]
                # Try to break at last period/exclamation/question within 500 chars
                last_dot = max(content_snippet.rfind('.'), content_snippet.rfind('!'), content_snippet.rfind('?'))
                if last_dot > 200: # Ensure we have a decent amount of text
                    result["summary"] = content_snippet[:last_dot + 1]
                else:
                    result["summary"] = content_snippet
                
                if len(result["content"]) > 500 and not result["summary"].endswith("..."):
                    if not result["summary"].endswith(('.', '!', '?')):
                        result["summary"] += "..."

        return result

    except Exception as e:
        print(f"Error parsing {url}: {e}")
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

        for topic, start_url in CATEGORIES.items():
            print(f"\n=== Processing Topic: {topic} ===")
            
            for page in range(1, MAX_PAGES_PER_TOPIC + 1):
                page_url = f"{start_url}?page={page}"
                print(f"  -> Crawling Page {page}: {page_url}")
                
                try:
                    resp = session.get(page_url, timeout=30)
                    if resp.status_code != 200:
                        print(f"    Failed to load page {page}")
                        continue
                        
                    soup = BeautifulSoup(resp.text, "html.parser")
                    
                    # Extract links
                    # Can Tho structure often lists articles in .news-item or similar blocks
                    # We look for links within specific containers or generally in the main column
                    
                    links = []
                    # Extract links using specific selectors we found earlier
                    # .ArticleHeader a, .PostTitle a, h2 a
                    articles = soup.select(".ArticleHeader a, .PostTitle a, h2 a, .title-news a, .news-title a")
                    
                    if not articles:
                         # Fallback to finding 'a' tags in main content if using class names failed
                         main_col = soup.find(class_="col-main") or soup.find(id="main")
                         if main_col:
                             articles = main_col.find_all("a", href=True)

                    for item in articles:
                        # If the item is already an 'a' tag (from select)
                        if item.name == "a":
                            a_tag = item
                        else:
                            a_tag = item.find("a")
                            
                        if a_tag and a_tag.get("href"):
                            full_url = normalize_url(a_tag["href"])
                            if full_url and full_url not in seen_urls and BASE_URL in full_url:
                                # Filter out noise
                                if any(x in full_url for x in ["/so-do-cong", "javascript:", "#", "mailto:", "signin", "login"]):
                                    continue
                                # Filter out likely category/pagination links (often short)
                                # But be careful, some articles have short slugs.
                                
                                links.append(full_url)
                    
                    # Deduplicate list while preserving order
                    links = list(dict.fromkeys(links))
                    print(f"    Found {len(links)} potential links.")
                    
                    if not links:
                        print("    No links found. Stopping topic or checking next page.")
                        # If page 1 has no links, it's weird. If page 10 has no links, maybe end of pagination.
                        if page > 1:
                            break
                    
                    count_saved = 0
                    for link in links:
                        if link in seen_urls:
                            continue
                            
                        seen_urls.add(link)
                        data = parse_article(link, topic)
                        
                        if data and data["title"] and data["content"]:
                            writer.writerow(data)
                            count_saved += 1
                            # print(f"      Saved: {data['title'][:40]}...")
                        time.sleep(SLEEP)
                    
                    print(f"    Saved {count_saved} articles.")
                    
                    if count_saved == 0 and len(links) == 0:
                        break
                        
                except Exception as e:
                    print(f"    Error on page {page}: {e}")
                    
                time.sleep(1)

if __name__ == "__main__":
    main()
