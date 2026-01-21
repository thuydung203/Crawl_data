import requests
import json
import csv
import re
import time
import html
from bs4 import BeautifulSoup
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning

# Suppress SSL warnings
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Configuration
BASE_URL = "https://congan.hanoi.gov.vn"
API_LIST_ENDPOINT = f"{BASE_URL}/DesktopModules/cmsview/api/NewsContent/listnew"
API_DETAIL_ENDPOINT = f"{BASE_URL}/DesktopModules/cmsview/api/NewsContent/getbyid"
OUTPUT_FILE = "congan_hanoi_data.csv"
MAX_PAGES_PER_TOPIC = 1000 # Increased to get all pages
PAGE_SIZE = 20 

# Topics to Crawl
TOPIC_CONFIG = {
    "Chỉ đạo điều hành": {
        "url": "https://congan.hanoi.gov.vn/chi-dao-dieu-hanh",
        "module_id": 391
    },
    "Tin ANTT và Cảnh báo tội phạm": {
        "url": "https://congan.hanoi.gov.vn/tin-antt-va-canh-bao-toi-pham",
        "module_id": 698
    },
    "An toàn giao thông": {
        "url": "https://congan.hanoi.gov.vn/an-toan-giao-thong",
        "module_id": 419
    },
    "Công tác PCCC và CNCH": {
        "url": "https://congan.hanoi.gov.vn/cong-tac-pccc-va-cnch",
        "module_id": 387
    },
    "Xây dựng Đảng và XDLL": {
        "url": "https://congan.hanoi.gov.vn/xay-dung-dang-va-xdll",
        "module_id": 422
    },
    "Phản hồi thông tin báo chí": {
        "url": "https://congan.hanoi.gov.vn/phan-hoi-thong-tin-bao-chi",
        "module_id": 432
    },
    "Phổ biến giáo dục pháp luật": {
        "url": "https://congan.hanoi.gov.vn/pho-bien-giao-duc-phap-luat",
        "module_id": 941
    },
    "Cải cách thủ tục hành chính": {
        "url": "https://congan.hanoi.gov.vn/cai-cach-thu-tuc-hanh-chinh",
        "module_id": 540
    },
    "Ngày kỷ niệm": {
        "url": "https://congan.hanoi.gov.vn/ngay-ky-niem",
        "module_id": 534
    },
    "Gương người tốt việc tốt": {
        "url": "https://congan.hanoi.gov.vn/guong-nguoi-tot-viec-tot",
        "module_id": 504
    },
    "Hoạt động của các đơn vị": {
        "url": "https://congan.hanoi.gov.vn/hoat-dong-cua-cac-don-vi",
        "module_id": 426
    }
}
# Headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": BASE_URL,
    "Origin": BASE_URL
}

session = requests.Session()
session.headers.update(HEADERS)

def get_module_id(url):
    try:
        print(f"  Analysing {url} for ModuleId...")
        resp = session.get(url, verify=False, timeout=30)
        resp.raise_for_status()
        matches = re.findall(r"myService\.getNewsBlockContents\(0,\s*(\d+),\s*3,\s*([^,]+)", resp.text)
        
        main_module_id = None
        for mod_id, page_arg in matches:
            page_arg = page_arg.strip()
            if page_arg == '1':
                continue
            else:
                 main_module_id = mod_id
                 break
        return main_module_id
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return None

def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text)
    soup = BeautifulSoup(text, "html.parser")
    text = soup.get_text(separator=" ")
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_content(raw_html):
    if not raw_html:
        return ""
    
    soup = BeautifulSoup(raw_html, "html.parser")
    for script in soup(["script", "style", "iframe", "noscript"]):
        script.decompose()
    for img in soup.find_all("img"):
        img.decompose()
    for figure in soup.find_all("figure"):
        figure.decompose()
    
    # Remove em tags (often used for image captions)
    for em in soup.find_all("em"):
        em.decompose()
    
    # Remove p tags with text-align:center (often image captions)
    for p in soup.find_all("p", style=lambda x: x and "text-align" in x and "center" in x):
        p.decompose()
    
    for td in soup.find_all("td"):
        if td.find("img"):
             if len(td.get_text(strip=True)) < 50: 
                td.decompose()

    text = soup.get_text(separator=" ")
    
    # Remove common caption patterns
    text = re.sub(r'(Ảnh|Nguồn|Tác giả|Thực hiện)\s*:.*?(?=\s|$)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\((Ảnh|Nguồn):.*?\)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^\s*\(?VGP\)?\s*[-–]\s*', '', text)
    
    # Remove lines that look like captions (short lines with common caption words)
    lines = text.split('.')
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        # Skip lines that are likely captions (contain image-related keywords and are relatively short)
        if len(line) < 150 and any(keyword in line.lower() for keyword in ['ảnh:', 'nguồn:', 'tác giả:', 'minh họa', 'chụp màn hình']):
            continue
        if line:
            cleaned_lines.append(line)
    
    text = '. '.join(cleaned_lines)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def parse_date(date_str):
    if not date_str:
        return ""
    try:
        # Format: 2026-01-19T08:50:00 or 2026-01-19T08:50:00Z
        if "T" in date_str:
             dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
             return dt.strftime("%d/%m/%Y")
        else:
            return date_str
    except:
        return ""

def main():
    seen_urls = set()
    write_header = True
    
    # Resume Logic
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header:
                write_header = False
                for row in reader:
                    if len(row) > 3:
                        seen_urls.add(row[3]) # url is index 3
        print(f"Resuming... Found {len(seen_urls)} existing articles.")
    except FileNotFoundError:
        print("Starting new crawl...")
    
    with open(OUTPUT_FILE, 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["topic", "title", "summary", "url", "keywords", "public_time", "content"])
        
        for topic_name, config in TOPIC_CONFIG.items():
            print(f"Processing Topic: {topic_name}")
            
            module_id = config["module_id"]
            topic_url = config["url"]
                
            print(f"  > ModuleId: {module_id}")
            
            consecutive_seen = 0
            consecutive_empty_pages = 0  # Track pages with no new articles
            
            for page in range(1, min(MAX_PAGES_PER_TOPIC + 1, 51)):  # Limit to 50 pages max per topic
                print(f"    Fetching page {page} for '{topic_name}'...", end="\r")
                
                try:
                    params = {
                        "PortalId": 0,
                        "ModuleId": module_id,
                        "GetType": 3,
                        "PageIndex": page,
                        "txtKeyword": ""
                    }
                    
                    resp = session.get(API_LIST_ENDPOINT, params=params, verify=False, timeout=10)
                    if resp.status_code != 200:
                        print(f"    Page {page} failed with status {resp.status_code}")
                        break
                        
                    data = resp.json()
                    
                    # Combine both ListData (top section) and ListMoreData (bottom section)
                    articles = data.get("ListData") or []
                    more_articles = data.get("ListMoreData") or []
                    
                    # Merge both lists
                    all_articles = articles + more_articles
                    
                    if not all_articles:
                        print(f"    No articles on page {page}. Stopping topic.")
                        break
                    
                    # Track new articles on this page
                    new_articles_this_page = 0
                        
                    for art in all_articles:
                        art_id = art.get("Id")
                        slug = art.get("Url", "bai-viet")
                        full_url = f"{BASE_URL}/tin-tuc/{slug}-{art_id}"
                        
                        if full_url in seen_urls:
                            consecutive_seen += 1
                            continue
                        
                        # Found a new article
                        new_articles_this_page += 1
                            
                        # Extract info
                        title = clean_text(art.get("Name", ""))
                        summary = clean_text(art.get("Description", ""))
                        
                        # Public Time from List item
                        pub_date_raw = art.get("PublishTime") or art.get("CreatedTime")
                        public_time = parse_date(pub_date_raw)
                        
                        keywords = "" 
                        
                        # Content
                        detail_content = ""
                        try:
                            # Fetch detail
                            detail_params = {"id": art_id}
                            det_resp = session.get(API_DETAIL_ENDPOINT, params=detail_params, verify=False, timeout=8)
                            det_data = det_resp.json()
                            if isinstance(det_data, list) and det_data:
                                det_data = det_data[0]
                                
                            # Updated to use FullContent
                            raw_content = det_data.get("FullContent") or det_data.get("Body") or det_data.get("Content") or ""
                            detail_content = clean_content(raw_content)
                            
                            # Extract Keywords
                            # Priority 1: From HTML Meta Tag (User Requested)
                            try:
                                html_resp = session.get(full_url, verify=False, timeout=10)
                                if html_resp.status_code == 200:
                                    html_soup = BeautifulSoup(html_resp.text, "html.parser")
                                    meta_keywords = html_soup.find("meta", attrs={"name": "keywords"}) or html_soup.find("meta", attrs={"id": "MetaKeywords"})
                                    if meta_keywords:
                                        keywords = meta_keywords.get("content", "")
                            except Exception as e:
                                print(f"      Error fetching HTML for keywords: {e}")

                            # Priority 2: From API if HTML failed or empty
                            if not keywords:
                                keywords = det_data.get("MetaKeywords") or det_data.get("Keyword") or det_data.get("Keywords") or det_data.get("Tags") or ""
                                if isinstance(keywords, list):
                                    keywords = ", ".join(keywords)
                            
                            if not summary:
                                soup = BeautifulSoup(raw_content, "html.parser")
                                summary = clean_text(soup.get_text()[:300] + "...")
                        except Exception as e:
                            print(f"      Error fetching detail for {art_id}: {e}")
                            # Don't skip, just save what we have
                        
                        writer.writerow([
                            topic_name,
                            title,
                            summary,
                            full_url,
                            keywords,
                            public_time,
                            detail_content
                        ])
                        f.flush() # Flush immediately
                        seen_urls.add(full_url)
                        consecutive_seen = 0
                    
                    # Check if this page had any new articles
                    if new_articles_this_page == 0:
                        consecutive_empty_pages += 1
                        print(f"    Page {page} had 0 new articles (consecutive empty: {consecutive_empty_pages})")
                        if consecutive_empty_pages >= 3:
                            print(f"    No new articles for 3 consecutive pages. Moving to next topic.")
                            break
                    else:
                        consecutive_empty_pages = 0  # Reset counter when we find new articles
                        
                    if consecutive_seen > 100: # Stop if we've seen many duplicates in a row (handling overlap)
                         print(f"    Encountered {consecutive_seen} duplicates. Stopping topic assuming overlap/caught up.")
                         break
                        
                except Exception as e:
                    print(f"    Error on page {page}: {e}")
                    time.sleep(2)
            
            print(f"  Finished {topic_name}. Total collected: {len(seen_urls)}")
            
if __name__ == "__main__":
    main()
