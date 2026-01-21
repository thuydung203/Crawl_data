
import requests
from bs4 import BeautifulSoup
import csv
import urllib3
import re
from urllib.parse import urljoin
import time
import os
import html

urllib3.disable_warnings()

# Configuration
OUTPUT_FILE = 'khanhhoa_data_final.csv'
BASE_URL = 'https://khanhhoa.gov.vn'
MAX_PAGES = 5 # Adjust as needed, usually user cleans afterwards? Or unlimited?
# Let's set a safe limit or loop until end. User didn't specify limit. 50 is common.
MAX_PAGES = 20 

TOPICS = [
    ("Lanh Dao Tinh", "https://khanhhoa.gov.vn/vi/tin-hoat-dong-cua-lanh-dao-tinh"),
    ("So Nganh Dia Phuong", "https://khanhhoa.gov.vn/vi/tin-hoat-dong-so-nganh-dia-phuong"),
    ("Chinh Sach", "https://khanhhoa.gov.vn/vi/chinh-sach-va-cuoc-song"),
    ("Cong Dan", "https://khanhhoa.gov.vn/vi/tin-noi-bat-danh-cho-cong-dan"),
    ("Doanh Nghiep", "https://khanhhoa.gov.vn/vi/tin-noi-bat-danh-cho-doanh-nghiep"),
    ("Du Khach", "https://khanhhoa.gov.vn/vi/gioi-thieu-den-du-khach")
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}

def clean_text(text):
    if not text:
        return ""
    # Unescape HTML entities
    text = html.unescape(str(text))
    
    # Ultra-Aggressive removal of "Ảnh minh họa", "Ảnh:", "Nguồn:", etc.
    # Pattern for "Ảnh minh họa. (Ảnh: ...)" or "Ảnh: ..." or "Nguồn: ..."
    artifacts = [
        r'(?i)Ảnh minh họa\.?(\s*\(Ảnh:\s*.*?\))?', # matches "Ảnh minh họa. (Ảnh: ...)"
        r'(?i)Ảnh sưu tầm\.?',
        r'(?i)Nguồn:?\s*.*?(?=\.|$)',
        r'(?i)Theo:?\s*.*?(?=\.|$)',
        r'(?i)\(Ảnh:\s*.*?\)',
        r'(?i)\(Nguồn:\s*.*?\)',
        r'(?i)Ảnh:\s*.*?(?=\.|$)',
        r'(?i)Bài,\s*ảnh:\s*.*?(?=\.|$)'
    ]
    for pattern in artifacts:
        text = re.sub(pattern, '', text)

    # Remove control characters
    text = re.sub(r'[\x00-\x1f\x7f]', '', text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def get_detail(url):
    try:
        resp = requests.get(url, verify=False, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # 1. Title: .title-article, h1, or <title>
        title = ""
        title_tag = soup.select_one(".title-article") or soup.select_one("h1")
        if title_tag:
            title = clean_text(title_tag.get_text())
        
        if not title and soup.title:
            title = clean_text(soup.title.string)

        if not title:
            # Fallback: Extract from script tag using regex
            match = re.search(r"['\"]og:title['\"]\s*[:=]\s*['\"](.*?)['\"]", resp.text)
            if match:
                title = clean_text(match.group(1))
            
        # Clean title: Remove site name prefix if it exists
        site_name = "Cổng Thông tin điện tử tỉnh Khánh Hòa"
        if title.startswith(site_name):
            # Remove "Cổng Thông tin điện tử tỉnh Khánh Hòa - " or "Cổng Thông tin điện tử tỉnh Khánh Hòa | "
            title = re.sub(f"^{re.escape(site_name)}\s*[-|:]\s*", "", title)
            # If it's just the site name exactly, keep it if nothing else found, but usually there's more.
            if title == site_name:
                 # Try finding from og:title if we still have only site name
                 og_title = soup.find("meta", property="og:title")
                 if og_title:
                     title = clean_text(og_title.get("content", ""))
            
        # 2. Summary: og:description or description
        summary = ""
        # Try multiple meta selectors for summary, prioritize id="ogdescription" as per user
        meta_desc = (soup.find("meta", id="ogdescription") or 
                     soup.find("meta", attrs={"property": "og:description"}) or 
                     soup.find("meta", attrs={"name": "og:description"}) or
                     soup.find("meta", attrs={"name": "description"}))
        if meta_desc:
            summary = clean_text(meta_desc.get("content", ""))
        
        if not summary:
            # Fallback: Extract from script tag using regex
            # matches 'og:description': '...' or "description": "..."
            match = re.search(r"['\"](?:og:)?description['\"]\s*[:=]\s*['\"](.*?)['\"]", resp.text)
            if match:
                summary = clean_text(match.group(1))
            
        # 3. URL: rel="canonical"
        canonical = ""
        link_canon = soup.find("link", rel="canonical")
        if link_canon:
            canonical = link_canon.get("href", "")
        if not canonical:
            canonical = url # Fallback
            
        # 4. Keywords: name="keywords" or "news_keywords"
        keywords = ""
        meta_kw = soup.find("meta", attrs={"name": "keywords"}) or soup.find("meta", attrs={"name": "news_keywords"})
        if meta_kw:
            keywords = clean_text(meta_kw.get("content", ""))
            
        # 5. Public Time: #datearticle, .detail-time, etc.
        public_time = ""
        # Try multiple selectors for time
        time_tag = (soup.select_one("#datearticle") or 
                    soup.select_one(".detail-time") or 
                    soup.select_one(".date") or 
                    soup.select_one(".time") or 
                    soup.select_one(".ngaythang"))
        if time_tag:
            raw_time = clean_text(time_tag.get_text())
            # Extract only date dd/mm/yyyy
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', raw_time)
            if date_match:
                public_time = date_match.group(1)
            else:
                public_time = raw_time
            
        # 6. Content: .chitietbaiviet, .detail-content, etc.
        content = ""
        # Try multiple selectors for content
        content_div = (soup.select_one(".chitietbaiviet") or 
                       soup.select_one(".detail-content.afcbc-body.clearfix") or 
                       soup.select_one(".tinmoii") or 
                       soup.select_one(".detail-content") or 
                       soup.select_one("#box_t"))
        
        if content_div:
            # Remove h2 within content
            for h2 in content_div.find_all("h2"):
                h2.decompose()
            
            # Remove scripts, styles
            for script in content_div(["script", "style"]):
                script.decompose()
                
            content = clean_text(content_div.get_text(separator=' ', strip=True))
            
        return {
            "title": title,
            "summary": summary,
            "url": canonical,
            "keywords": keywords,
            "public_time": public_time,
            "content": content
        }
        
    except Exception as e:
        print(f"Error extracting detail {url}: {e}")
        return None

def crawl():
    # Init file
    with open(OUTPUT_FILE, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["topic", "title", "summary", "url", "keywords", "public_time", "content"])
        writer.writeheader()

    seen_urls = set()

    for topic_name, topic_url in TOPICS:
        print(f"--- Crawling Topic: {topic_name} ---")
        
        session = requests.Session()
        session.headers.update(HEADERS)
        
        # Initial Request
        try:
            resp = session.get(topic_url, verify=False, timeout=20)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            current_page = 1
            
            while current_page <= MAX_PAGES:
                print(f"  Processing Page {current_page}...")
                
                # Extract Links
                # Selector inference: usually generic link in main container. 
                # Let's target links that look like article links (have IDs or .html) or are within a list structure.
                # Assuming generic list or based on observation 'tin-hoat-dong-cua-lanh-dao-tinh' path segments
                
                links_found = 0
                # Inspect soup for article links. 
                # We can filter by URL pattern: typically /vi/.../something-article-slug
                # Avoiding pagination links (javascript:...) and category links
                
                anchors = soup.find_all("a", href=True)
                page_links = []
                for a in anchors:
                    href = a['href']
                    full_url = urljoin(resp.url, href)
                    
                    if "javascript:" in href: continue
                    
                    # Basic check for article URLs (deep path or .html)
                    # And match topic path if possible
                    is_valid = Topic_Url_Base_Check(full_url, topic_url)
                    
                    if is_valid: 
                        if full_url not in seen_urls and len(href) > 20:
                            page_links.append(full_url)
                
                # Filter unique on page
                unique_page_links = []
                for link in page_links:
                     if link not in unique_page_links:
                         unique_page_links.append(link)
                
                print(f"    Found {len(unique_page_links)} new articles.")
                
                count_saved = 0
                with open(OUTPUT_FILE, 'a', encoding='utf-8-sig', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=["topic", "title", "summary", "url", "keywords", "public_time", "content"])
                    for link in unique_page_links:
                        if link in seen_urls: continue
                        
                        seen_urls.add(link)
                        # We only crawl if it looks like an article
                        data = get_detail(link)
                        if data and data['content']:
                            # Title is important
                            if not data['title']:
                                # Try fallback if still empty
                                pass
                            
                            # Filter: Must have title, content AND summary
                            if data['title'] and data['summary']:
                                data['topic'] = topic_name
                                writer.writerow(data)
                                count_saved += 1
                                # Print more info to verify quality
                                print(f"      Saved: {data['title'][:40]}... (Summ len: {len(data['summary'])})")
                            else:
                                if not data['summary']:
                                    print(f"      Skipped (No summary): {data['title'][:40]}...")
                                elif not data['title']:
                                    print(f"      Skipped (No title): {link}")
                        else:
                            # Might be a category link or irrelevant
                            pass
                            
                if count_saved == 0 and len(unique_page_links) == 0:
                     print("    No links found. Stopping topic.")
                     break
                
                # Pagination Logic (PostBack)
                # Find Link for Next Page (Current + 1)
                next_page_num = current_page + 1
                
                # Look for <a> with text == str(next_page_num) or title="Next" etc.
                # Note: The visible text might be "2", "3", etc.
                target_link = None
                
                # Find by exact text
                pager_link = soup.find('a', string=str(next_page_num))
                if not pager_link:
                    # Maybe it's "..." or "Next" or ">"
                    # But reliable way is number. If not found, maybe we reached end of visible range?
                    pass
                
                if pager_link and 'javascript:__doPostBack' in pager_link.get('href', ''):
                    target_link = pager_link
                
                if not target_link:
                    print(f"    Pagination link for page {next_page_num} not found. Stopping.")
                    break
                    
                # Extract PostBack params
                href = target_link['href']
                # format: javascript:__doPostBack('TARGET','ARGUMENT')
                match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
                if not match:
                    break
                    
                event_target = match.group(1)
                event_argument = match.group(2)
                
                viewstate = soup.find(id="__VIEWSTATE")['value']
                viewstategenerator = soup.find(id="__VIEWSTATEGENERATOR")
                eventvalidation = soup.find(id="__EVENTVALIDATION")
                
                payload = {
                    '__EVENTTARGET': event_target,
                    '__EVENTARGUMENT': event_argument,
                    '__VIEWSTATE': viewstate,
                    '__VIEWSTATEGENERATOR': viewstategenerator['value'] if viewstategenerator else '',
                    '__EVENTVALIDATION': eventvalidation['value'] if eventvalidation else ''
                }
                
                # Post
                try:
                    p_resp = session.post(topic_url, data=payload, verify=False, timeout=20)
                    soup = BeautifulSoup(p_resp.content, 'html.parser')
                    current_page += 1
                    time.sleep(1)
                except Exception as e:
                    print(f"    Pagination Error: {e}")
                    break
                    
        except Exception as e:
            print(f"  Error on topic {topic_name}: {e}")

def Topic_Url_Base_Check(full_url, topic_url):
    # Determine if link belongs to sub-path of website, roughly
    # topic_url: https://khanhhoa.gov.vn/vi/tin-hoat-dong-cua-lanh-dao-tinh
    # full_url should usually start with https://khanhhoa.gov.vn/vi/
    # And ideally resemble the topic path or be an article
    return "khanhhoa.gov.vn/vi/" in full_url and full_url != topic_url

if __name__ == "__main__":
    crawl()
