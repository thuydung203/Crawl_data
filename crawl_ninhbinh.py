import csv
import time
import os
import re
import requests
from bs4 import BeautifulSoup
import urllib3
from urllib.parse import urljoin, urlparse

urllib3.disable_warnings()

# --- Configuration ---
OUTPUT_FILE = "ninhbinh_data_final.csv"
MAX_PAGES_PER_TOPIC = 50


# Topics
TOPICS = [
    ("Kinh tế", "https://ninhbinh.gov.vn/kinh-te"),
    ("Văn hóa - Xã hội", "https://ninhbinh.gov.vn/van-hoa-xa-hoi"),
    ("An ninh - Quốc phòng", "https://ninhbinh.gov.vn/an-ninh-quoc-phong"),
    ("Thông tin chỉ đạo", "https://ninhbinh.gov.vn/thong-tin-chi-dao-dieu-hanh"),
    ("Tin trong nước - Quốc tế", "https://ninhbinh.gov.vn/tin-trong-nuoc-quoc-te"),
    ("Cải cách hành chính", "https://ninhbinh.gov.vn/tin-tuc-cai-cach-hanh-chinh"),
    ("Sở KHCN - Hoạt động tỉnh", "https://sokhcn.ninhbinh.gov.vn/tin-hoat-dong-cua-tinh"),
    ("Sở KHCN - Hoạt động sở", "https://sokhcn.ninhbinh.gov.vn/tin-hoat-dong-cua-so"),
    ("Sở KHCN - Tin KHCN", "https://sokhcn.ninhbinh.gov.vn/tin-khcn-trong-nuoc")
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}

def clean_text(text):
    if not text: return ""
    text = re.sub(r'[\x00-\x1f\x7f]', '', str(text))
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def get_detail_content(url):
    try:
        resp = requests.get(url, verify=False, timeout=20, headers=headers)
        if resp.status_code != 200:
            return "", "", "", "", ""
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Detail Selectors
        # Title: .ArticleHeader
        title = ""
        title_tag = soup.select_one('.ArticleHeader') or soup.select_one('.Title') or soup.find('h1')
        if title_tag:
            title = clean_text(title_tag.get_text())
            
        # Date: .PostDate
        public_time = ""
        date_tag = soup.select_one('.PostDate') or soup.find("div", class_="date")
        if date_tag:
             public_time = clean_text(date_tag.get_text())
        
        # Keywords
        keywords = ""
        meta_keywords = soup.find("meta", {"name": "keywords"})
        if meta_keywords:
            keywords = meta_keywords.get("content", "")
            
        # Content: .ArticleContent
        content_div = soup.select_one('.ArticleContent') or soup.select_one('.news-content') or soup.select_one('#news-content')
        content = ""
        summary = ""
        
        if content_div:
            # Clean scripts and specific garbage
            for script in content_div(["script", "style", "iframe", "div.relate-news", "div.box-relate"]):
                script.decompose()
            
            # Summary: .ArticleSummary or first bold
            # In Ninh Binh, summary is often outside content in .ArticleSummary
            summary_div = soup.select_one('.ArticleSummary')
            if summary_div:
                summary = clean_text(summary_div.get_text())
            else:
                 strong_tag = content_div.select_one('p > strong') or content_div.select_one('strong')
                 if strong_tag:
                    summary = clean_text(strong_tag.get_text())
            
            # Content Flattening
            raw_content = content_div.get_text(separator=' ', strip=True)
            
            # Cleaning phrases
            # Enhanced cleaning for "Ảnh minh họa" and sources
            if "Tác giả:" in raw_content:
                raw_content = raw_content.split("Tác giả:")[0]
            
            # List of patterns to remove (matched with clean_ninhbinh.py)
            patterns = [
                # Ultra-aggressive approach - unconditional removal of phrases
                r'Ảnh minh ho[ạa]', 
                r'Ảnh sưu tầm',
                r'Nguồn\s*:.*?(?:\n|$|\))',
                r'Theo\s*:.*?(?:\n|$|\))',
                r'Ảnh\s*:.*?(?:\n|$|\))',
                r'\(\s*Nguồn.*?\)',
                r'Nguồn\s*:.*?(?:\n|$)',
                r'Theo\s+.*?(?:\n|$)',
                r'\([^)]*?(?:ảnh|nguồn).*?\)',
                r'Ảnh\s*:',
            ]
            
            for p in patterns:
                raw_content = re.sub(p, '', raw_content, flags=re.IGNORECASE | re.DOTALL)
            
            raw_content = re.sub(r'\.\s*\.', '.', raw_content)
                
            content = clean_text(raw_content)
            
            if not summary and content:
                summary = content[:200] + "..."

        return title, summary, keywords, public_time, content

    except Exception as e:
        print(f"    Error detail {url}: {e}")
        return "", "", "", "", ""

def crawl():
    seen_urls = set()
    
    # Init file (overwrite)
    with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
    global_seen = set()
    
    for topic_name, topic_url in TOPICS:
        print(f"--- Processing {topic_name} ---")
        try:
            # 1. Fetch First Page (GET) & Extract IDs
            resp = requests.get(topic_url, verify=False, timeout=20, headers=headers)
            if resp.status_code != 200:
                print(f"  Failed to load list page {topic_url}")
                continue
                
            content_text = resp.text
            soup = BeautifulSoup(content_text, 'html.parser')
            
            # Extract IDs for pagination
            # Look for regex patterns for article_category_id and site_id
            cat_id_match = re.search(r'article_category_id\s*[:=]\s*["\']?(\d+)["\']?', content_text, re.IGNORECASE)
            site_id_match = re.search(r'site_id\s*[:=]\s*["\']?(\d+)["\']?', content_text, re.IGNORECASE)
            
            article_category_id = cat_id_match.group(1) if cat_id_match else None
            site_id = site_id_match.group(1) if site_id_match else None
            
            print(f"  Detected IDs - Category: {article_category_id}, Site: {site_id}")
            
            # Process Page 1 Articles (from static HTML)
            current_page_urls = []
            parsed_topic_url = urlparse(topic_url)
            base_domain = f"{parsed_topic_url.scheme}://{parsed_topic_url.netloc}"
            
            # Selectors
            containers = soup.select(".UIListNews_Default .item a") + soup.select(".list-news .item a")
            if not containers: containers = soup.find_all("a")
            
            for a in containers:
                href = a.get('href')
                if not href: continue
                full_url = urljoin(base_domain, href)
                 # Filter: match article ID pattern or html
                if re.search(r'(-\d+|\.html)$', full_url.split('?')[0]) and len(full_url) > 30 and full_url not in global_seen:
                    if full_url not in current_page_urls:
                        current_page_urls.append(full_url)
            
            print(f"  Page 1: Found {len(current_page_urls)} articles.")
            
            # Save Page 1
            with open(OUTPUT_FILE, "a", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                for link in current_page_urls:
                    if link in global_seen: continue
                    title, summary, keywords, public_time, content = get_detail_content(link)
                    if title and content:
                        writer.writerow({
                            "topic": topic_name, "title": title, "summary": summary,
                            "url": link, "keywords": keywords, "public_time": public_time, "content": content
                        })
                        global_seen.add(link)
            
            # 2. Pagination Loop (POST)
            if article_category_id and site_id:
                api_url = f"{base_domain}/DesktopModule/UIArticleInMenu/ArticleInMenuPagination.aspx/LoadArticle"
                
                # Pages 2 to MAX
                for page in range(2, MAX_PAGES_PER_TOPIC + 1):
                    print(f"  Fetching Page {page} via API...")
                    # Payload
                    payload = {
                        "article_category_id": article_category_id,
                        "site_id": site_id,
                        "page": page,
                        "page_size": 15, # Default usually 10-15
                        "keyword": "", "date_begin": "", "date_end": "",
                        "show_no": "False", "show_post_date": "False", "num_of_text": 0,
                        "show_view_count": "False", "filter_order_in_list": "False",
                        "is_default": "False", "new": "False", "number_of_day": 3, "no": -5,
                        "lang": "vi-VN"
                    }
                    
                    try:
                        p_resp = requests.post(api_url, data=payload, verify=False, headers=headers, timeout=20)
                        if p_resp.status_code != 200:
                            print(f"    API Error {p_resp.status_code}")
                            break
                            
                        # Response is HTML fragment
                        p_soup = BeautifulSoup(p_resp.content, 'html.parser')
                        p_links = []
                        
                        # Extract links from fragment
                        # Fragment usually contains just list items
                        p_containers = p_soup.find_all("a")
                        
                        for a in p_containers:
                            href = a.get('href')
                            if not href: continue
                            full_url = urljoin(base_domain, href)
                            if re.search(r'(-\d+|\.html)$', full_url.split('?')[0]) and len(full_url) > 30:
                                if full_url not in p_links:
                                    p_links.append(full_url)
                        
                        if not p_links:
                            print("    No articles found on this page. End of topic.")
                            break
                            
                        print(f"    Found {len(p_links)} new articles.")
                        
                        # Save
                        new_on_page = 0
                        with open(OUTPUT_FILE, "a", encoding="utf-8-sig", newline="") as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            for link in p_links:
                                if link in global_seen: continue
                                title, summary, keywords, public_time, content = get_detail_content(link)
                                if title and content:
                                    writer.writerow({
                                        "topic": topic_name, "title": title, "summary": summary,
                                        "url": link, "keywords": keywords, "public_time": public_time, "content": content
                                    })
                                    global_seen.add(link)
                                    new_on_page += 1
                        
                        print(f"    Saved {new_on_page} items.")
                        if new_on_page == 0:
                             # If we found links but all were seen, probably overlapping or done
                             # But let's check duplicates
                             pass
                            
                        time.sleep(1)
                        
                    except Exception as e:
                        print(f"    Error on page {page}: {e}")
                        break

        except Exception as e:
            print(f"  Error processing topic {topic_name}: {e}")

if __name__ == "__main__":
    crawl()
