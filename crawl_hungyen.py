import requests
import csv
import time
import re
from bs4 import BeautifulSoup
import urllib3
import ssl

urllib3.disable_warnings()

# Configuration
OUTPUT_FILE = "hungyen_data_final.csv"
MAX_ITEMS_PER_TOPIC = 2000
MAX_PAGES_PER_TOPIC = 100 # Increased limit

# Topics from user
# Format: Name -> First Page URL
TOPIC_URLS = [
    ("Lắng nghe truyền thông", "https://hungyen.gov.vn/chuyen-muc-lang-nghe-truyen-thong-c21072-1.html"),
    ("Lịch sử - Văn hóa - Du lịch", "https://hungyen.gov.vn/chuyen-muc-lich-su-van-hoa-du-lich-c28-1.html"),
    ("Tin tổng hợp", "https://hungyen.gov.vn/chuyen-muc-tin-tong-hop-c21073-1.html"),
    ("Bầu cử ĐBQH", "https://hungyen.gov.vn/chuyen-muc-bau-cu-dai-bieu-quoc-hoi-khoa-xvi-c21087-1.html"),
    ("Tin sở ngành xã phường", "https://hungyen.gov.vn/chuyen-muc-tin-cac-so-nganh-xa-phuong-c216-1.html"),
    ("Tin chung toàn tỉnh", "https://hungyen.gov.vn/chuyen-muc-tin-chung-toan-tinh-c217-1.html"),
    ("Tin quốc tế", "https://hungyen.gov.vn/chuyen-muc-tin-quoc-te-c220-1.html"),
    ("Tin trong nước", "https://hungyen.gov.vn/chuyen-muc-tin-trong-nuoc-c221-1.html"),
    ("Văn bản chính sách mới", "https://hungyen.gov.vn/chuyen-muc-vb-chinh-sach-moi-c223-1.html")
]

class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        ctx.check_hostname = False
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize, block=block,
            ssl_version=ssl.PROTOCOL_TLSv1_2, ssl_context=ctx
        )

session = requests.Session()
session.mount('https://', CustomHttpAdapter())
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
})

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def get_detail_content(url):
    try:
        resp = session.get(url, verify=False, timeout=20)
        if resp.status_code != 200:
            return "", "", ""
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Content container
        # Browser inspection: .article-content #container or .new-detail-layout-type-2
        content_div = soup.select_one('.article-content') or soup.select_one('#container') or soup.select_one('.new-detail-layout-type-2')
        
        content = ""
        summary_text = ""
        
        if content_div:
            # Remove scripts, styles
            for script in content_div(["script", "style", "iframe", "div.relate-news"]):
                script.decompose()
            
            # Extract content with standard formatting
            # Extract content with standard formatting
            # User wants clear content, usually paragraphs
            # We can use get_text with separator to keep formatting but flatten to one line
            raw_content = content_div.get_text(separator=' ', strip=True)
            # Remove "Tác giả:" and everything after it
            if "Tác giả:" in raw_content:
                raw_content = raw_content.split("Tác giả:")[0]
            content = clean_text(raw_content)
            
            # Helper for summary: specifically look for the first bold paragraph or strong tag
            # User screenshot shows: <p ..><strong>...</strong></p>
            # Let's find the first valid strong text
            strong_tag = content_div.select_one('p > strong')
            if not strong_tag:
                strong_tag = content_div.select_one('strong')
                
            if strong_tag:
                 summary_text = clean_text(strong_tag.get_text())

        # Keywords
        keywords = ""
        meta_keywords = soup.find("meta", {"name": "keywords"})
        if meta_keywords:
            keywords = meta_keywords.get("content", "")
            
        # Public time
        public_time = ""
        time_tag = soup.select_one('.post-date')
        if time_tag:
             raw_time = clean_text(time_tag.get_text())
             # Remove | lượt xem: ...
             public_time = raw_time.split('|')[0].strip()
             # Also clean if it says "Lượt xem" without pipe
             public_time = re.split(r'lượt xem', public_time, flags=re.IGNORECASE)[0].strip()
        else:
            for div in soup.find_all('div'):
                txt = clean_text(div.get_text())
                if "lượt xem:" in txt.lower():
                    # extract date part ? likely at start
                    public_time = txt.split('|')[0].strip()
                    public_time = re.split(r'lượt xem', public_time, flags=re.IGNORECASE)[0].strip()
                    if public_time:
                        break

        return content, keywords, public_time, summary_text
    except Exception as e:
        print(f"    Error fetching detail {url}: {e}")
        return "", "", "", ""
    except Exception as e:
        print(f"    Error fetching detail {url}: {e}")
        return "", "", ""

def crawl():
    seen_urls = set()
    
    # Check existing
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "url" in row: seen_urls.add(row["url"])
        print(f"Loaded {len(seen_urls)} existing articles.")
    except: pass
    
    mode = 'a' if seen_urls else 'w'
    with open(OUTPUT_FILE, mode, encoding="utf-8-sig", newline="") as f:
        fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == 'w': writer.writeheader()
        
        for topic_name, first_url in TOPIC_URLS:
            print(f"--- Processing {topic_name} ---")
            
            # Derive base URL pattern
            # e.g. ...-c21072-1.html -> prefix: ...-c21072, suffix: .html
            match = re.search(r"(.+-\w+)-1\.html$", first_url)
            if not match:
                print(f"  Skipping {topic_name}, URL format not standard.")
                continue
                
            base_url = match.group(1)
            
            page = 1
            items_fetched = 0
            
            while page <= MAX_PAGES_PER_TOPIC and items_fetched < MAX_ITEMS_PER_TOPIC:
                visit_url = f"{base_url}-{page}.html"
                print(f"  Fetching {visit_url}")
                
                try:
                    resp = session.get(visit_url, verify=False, timeout=20)
                    if resp.status_code != 200:
                        print(f"    Page {page} failed or ended (Status {resp.status_code})")
                        break
                        
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    
                    # Find articles
                    # Selector from browser inspection: div.article-right.show-left (container?)
                    # Let's try to find the list items. Usually class "news-item" or similar.
                    # Re-inspecting provided output:
                    # Found "article-title common-title"
                    
                    articles = soup.select('div.item-new') # Common structure?
                    if not articles:
                        # Try finding titles directly and getting parent
                        titles = soup.select('a.article-title')
                        if not titles:
                            print("    No articles found on page.")
                            break
                        articles = [t.find_parent('div') for t in titles] # Heuristic
                    
                    if not articles:
                        print("    No article containers found.")
                        break
                        
                    new_items_on_page = 0
                    for art in articles:
                        # Title
                        title_tag = art.select_one('a.article-title')
                        if not title_tag: continue
                        
                        title = clean_text(title_tag.get_text())
                        link = title_tag.get('href')
                        if not link: continue
                        
                        # Absolutize link
                        if not link.startswith('http'):
                            link = "https://hungyen.gov.vn" + link
                            
                        if link in seen_urls: continue
                        
                        # Summary
                        summary_tag = art.select_one('div.article-brief')
                        summary = clean_text(summary_tag.get_text()) if summary_tag else ""
                        
                        # Date
                        date_tag = art.select_one('span.article-date')
                        # Sometimes date is text node
                        public_time = clean_text(date_tag.get_text()) if date_tag else ""
                        
                        # Detail Page
                        content, keywords, detailed_time, detail_summary = get_detail_content(link)
                        
                        # Use detailed time if available
                        final_time = detailed_time if detailed_time else public_time
                        
                        # Use detail summary if found (it's the bold text), else list summary, else fallback
                        final_summary = detail_summary if detail_summary else summary
                        if not final_summary and content:
                             final_summary = content[:200] + "..."

                        row = {
                            "topic": topic_name,
                            "title": title,
                            "summary": final_summary,
                            "url": link,
                            "keywords": keywords,
                            "public_time": final_time,
                            "content": content
                        }
                        
                        writer.writerow(row)
                        seen_urls.add(link)
                        new_items_on_page += 1
                        items_fetched += 1
                    
                    f.flush()
                    print(f"    Saved {new_items_on_page} items on page {page}.")
                    
                    if new_items_on_page == 0:
                        print("    No new items found on page, likely duplicate or empty.")
                        # Check if we should stop? Maybe just continue if it's overlap
                        # If page has 0 articles extracted, we break
                        if not soup.select('a.article-title'):
                            break
                    
                    page += 1
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"    Error on page {page}: {e}")
                    break

if __name__ == "__main__":
    crawl()
