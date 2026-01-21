import csv
import time
import os
import html
import re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib3.exceptions import InsecureRequestWarning

# Suppress SSL warnings
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Configuration
BASE_URL = "https://conganthanhhoa.gov.vn"
OUTPUT_FILE = "thanhhoa_data_final.csv"
MAX_PAGES_PER_TOPIC = 100 
PAGE_SIZE = 12

# Topic Configuration (Vietnamese Names)
TOPIC_CONFIG = {
    "Tin ANTT trong tỉnh": {
        "url": "https://conganthanhhoa.gov.vn/tin-tuc-su-kien/tin-an-ninh-trat-tu/tin-antt-trong-tinh",
    },
    "Tin trong nước": {
        "url": "https://conganthanhhoa.gov.vn/tin-tuc-su-kien/tin-an-ninh-trat-tu/tin-trong-nuoc",
    },
    "Vì nhân dân phục vụ": {
        "url": "https://conganthanhhoa.gov.vn/tin-tuc-su-kien/tin-hoat-dong/vi-nhan-dan-phuc-vu",
    },
    "Phổ biến giáo dục pháp luật": {
        "url": "https://conganthanhhoa.gov.vn/tin-tuc-su-kien/tin-hoat-dong/pho-bien-giao-duc-phap-luat",
    },
    "Đề án 06": {
        "url": "https://conganthanhhoa.gov.vn/de-an-06",
    },
    "Chống diễn biến hòa bình": {
        "url": "https://conganthanhhoa.gov.vn/tin-tuc-su-kien/chong-dien-bien-hoa-binh",
    },
    "Học tập và làm theo Bác": {
        "url": "https://conganthanhhoa.gov.vn/tin-tuc-su-kien/tin-trong-nuoc-va-the-gioi/hoc-tap-va-lam-theo-tu-tuong-dao-duc-phong-cach-ho-chi-minh",
    },
    "Hướng về cơ sở": {
        "url": "https://conganthanhhoa.gov.vn/tin-tuc-su-kien/huong-ve-co-so2",
    },
    "80 năm Ngày Truyền thống CAND": {
        "url": "https://conganthanhhoa.gov.vn/tin-tuc-su-kien/tu-lieu/80-nam-ngay-truyen-thong-cand",
    }
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

session = requests.Session()
session.headers.update(HEADERS)

def init_driver():
    chrome_options = Options()
    # chrome_options.add_argument("--headless") # Optional: run visible to debug if needed
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text)
    soup = BeautifulSoup(text, "html.parser")
    text = soup.get_text(separator=" ")
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def parse_date(date_str):
    if not date_str:
        return ""
    # Helper to fix common Vietnamese date formats
    # "Thứ năm, 20/11/2025 | 14:24" => "20/11/2025"
    date_str = date_str.replace("-", "/")
    match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', date_str)
    if match:
        day, month, year = match.groups()
        return f"{int(day):02d}/{int(month):02d}/{year}"
    return date_str

def clean_content(raw_html):
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    
    # 1. Remove structure containers that often hold images+captions in VHV CMS
    # NOTE: We remove ALL tables because on news sites they are 95% used for layout/images.
    # If meaningful data is in tables, this might need adjustment, but user requested "super clean".
    for tag in soup(["table", "figure", "figcaption"]):
        tag.decompose()
        
    # 2. Remove unwanted tags
    for tag in soup(["script", "style", "iframe", "noscript", "meta", "link", "input", "button", "form", "video", "audio"]):
        tag.decompose()
        
    # 3. Remove images
    for img in soup(["img", "svg"]):
        img.decompose()
        
    # 4. Remove elements with specific image/caption related classes
    for tag in soup.select(".image, .img, .picture, .caption, .desc, .photo, .pic, .note-img, .expNoEdit"):
         tag.decompose()
        
    # 5. Remove remaining captions in em/i that look like captions
    for tag in soup.find_all(["em", "i"]):
        txt = tag.get_text().strip()
        # Heuristic: shorter text starting with keywords
        if len(txt) < 150 and (re.match(r'^(\()?Ảnh', txt, re.IGNORECASE) or re.match(r'^(\()?Nguồn', txt, re.IGNORECASE)):
             tag.decompose()
        
    # Get text
    text = soup.get_text(separator=" ")
    
    # Cleaning patterns for signature/source
    text = re.sub(r'(Ảnh|Nguồn|Tác giả|Thực hiện)\s*:.*?(?=\s|$)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\((Ảnh|Nguồn):.*?\)', '', text, flags=re.IGNORECASE)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove common boilerplate suffixes
    boilerplate = [
        r'Chia sẻ Lưu',
        r'Tổng số điểm của bài viết là:.*',
        r'Đánh giá bài viết:.*',
        r'Bình chọn bài viết:.*'
    ]
    for pattern in boilerplate:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
    
    return text

def fetch_detail(url):
    try:
        resp = session.get(url, verify=False, timeout=10)
        if resp.status_code != 200:
            return None, None, None, None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Content
        # VHV: .detail-content, .content-detail, #content-detail, .noidung, .post-content, .article-content
        content_div = soup.select_one(".detail-content, .content-detail, #content-detail, .noidung, .post-content, .article-content")
        
        # Fallback for video/media posts
        if not content_div:
            content_div = soup.select_one(".col-md-9, article, main")
            
        content = ""
        if content_div:
            content = clean_content(str(content_div))
        
        # Keywords
        keywords = ""
        meta_kw = soup.find("meta", attrs={"name": "keywords"})
        if meta_kw:
            keywords = meta_kw.get("content", "")
            
        # Public Time from Detail
        public_time = ""
        time_tag = soup.select_one(".date, .time, .published-time, .post-date")
        if time_tag:
            public_time = parse_date(time_tag.get_text())
        
        # Title from Detail (h1)
        title = ""
        h1 = soup.select_one("h1.title, .title-detail, .article-detail-title, h1")
        if h1:
             title = clean_text(h1.get_text())

        return content, keywords, public_time, title
        
    except Exception as e:
        print(f"      Error detail {url}: {e}")
        return None, None, None, None

def main():
    write_header = True
    seen_urls = set()
    
    # Check if we should resume or start over
    # User requested "đúng tên topic" which implies old data is bad.
    # We will OVERWRITE the file to be safe and ensure clean data.
    print(f"Starting fresh crawl to {OUTPUT_FILE}...")
    
    driver = init_driver()
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["topic", "title", "summary", "url", "keywords", "public_time", "content"])
            
        for topic_name, config in TOPIC_CONFIG.items():
            print(f"Processing Topic: {topic_name}")
            url = config["url"]
            
            try:
                driver.get(url)
            except:
                print(f"  Error loading {url}")
                continue
            
            consecutive_seen = 0
            
            for page in range(1, MAX_PAGES_PER_TOPIC + 1):
                print(f"    Page {page}...", end="\r")
                
                # Check for articles
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.item, div.post-item, div.col-md-9"))
                    )
                except:
                    print(f"    Timeout - no articles on page {page}. Stopping topic.")
                    break
                    
                # Parsing HTML from Selenium
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Identify items (VHV usually .item inside #section...)
                items = soup.select("div.item")
                if not items:
                     items = soup.select(".post-item")
                     
                new_count = 0
                for item in items:
                    link = item.find("a")
                    if not link: continue
                    
                    href = link.get('href')
                    if not href: continue
                    
                    if href.startswith("/"):
                        href = BASE_URL + href
                        
                    # Filter bad links
                    if "conganthanhhoa.gov.vn" not in href or ".html" not in href:
                         continue
                        
                    if href in seen_urls:
                        consecutive_seen += 1
                        continue
                        
                    # Extract Summary/Title from List
                    list_title = clean_text(link.get_text())
                    # Try other title tags if link text is empty (image link)
                    if not list_title:
                        h_tag = item.find(["h2", "h3", "h4"])
                        if h_tag: list_title = clean_text(h_tag.get_text())
                        
                    summary_tag = item.select_one(".desc, .summary, .sapo")
                    list_summary = clean_text(summary_tag.get_text()) if summary_tag else ""
                    
                    time_tag = item.select_one(".time, .date")
                    list_time = parse_date(time_tag.get_text()) if time_tag else ""
                    
                    # Fetch Detail (Request)
                    content, keywords, detail_time, detail_title = fetch_detail(href)
                    
                    # FINAL FIELD SELECTION
                    # Title
                    title = detail_title if detail_title else list_title
                    if not title: title = "No Title" # Should not happen often
                    
                    # Time
                    public_time = detail_time if detail_time else list_time
                    
                    # Summary Strategy
                    summary = ""
                    # 1. Try meta description first if it's long and doesn't look like a truncated title
                    if meta_summary and len(meta_summary) > 100 and not meta_summary.endswith("..."):
                        summary = meta_summary
                    
                    # 2. Check list summary
                    if not summary:
                        if len(list_summary) > 250 and not list_summary.endswith("..."):
                            summary = list_summary
                            
                    # 3. Generate from content (Aggressive)
                    if not summary or len(summary) < 150 or summary.endswith("..."):
                        if content:
                            content_snippet = content[:550]
                            last_dot = max(content_snippet.rfind('.'), content_snippet.rfind('!'), content_snippet.rfind('?'))
                            if last_dot > 150:
                                summary = content_snippet[:last_dot + 1]
                            else:
                                summary = content_snippet[:500].strip()
                            
                            if len(content) > 550 and not summary.endswith("..."):
                                 if not summary.endswith(('.', '!', '?')):
                                     summary += "..."
                        else:
                            summary = list_summary # Fallback if no content
                    
                    # Write to CSV
                    writer.writerow([topic_name, title, summary, href, keywords, public_time, content])
                    f.flush()
                    seen_urls.add(href)
                    new_count += 1
                    consecutive_seen = 0
                    
                print(f"    Page {page}: Scraped {new_count} articles.")
                
                if new_count == 0 and consecutive_seen > 15:
                    print("    Stopping topic due to duplicates.")
                    break
                
                # Pagination - Click NEXT
                try:
                    next_page_idx = page + 1
                    
                    # 1. Try clicking number directly (most reliable for VHV)
                    next_btn = driver.find_elements(By.XPATH, f"//a[normalize-space()='{next_page_idx}']")
                    clicked = False
                    if next_btn:
                        driver.execute_script("arguments[0].click();", next_btn[0])
                        clicked = True
                    else:
                        # 2. Try 'Next' or '>>' or 'Tiếp'
                        next_arrow = driver.find_elements(By.CSS_SELECTOR, ".next, a[title='Next'], a[title='Tiếp']")
                        if next_arrow:
                            try:
                                driver.execute_script("arguments[0].click();", next_arrow[0])
                                clicked = True
                            except:
                                pass
                            
                    if not clicked:
                        print("    No next page button found. End of topic.")
                        break
                        
                    time.sleep(3) # Wait for AJAX
                    
                except Exception as e:
                    print(f"    Pagination Error: {e}")
                    break
                    
    driver.quit()
    print("Done.")

if __name__ == "__main__":
    main()
