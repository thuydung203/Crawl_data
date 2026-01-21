
import requests
import urllib3
import ssl
import csv
import time
import re
import html
from bs4 import BeautifulSoup
from urllib.parse import unquote

urllib3.disable_warnings()

# Configuration
OUTPUT_FILE = "vinhphuc_data_final.csv"
API_URL = "https://vinhphuc.gov.vn/APIVP/api/DSTinTuc/QUERYDATA"
PAGE_SIZE = 50
MAX_ITEMS_PER_CATEGORY = 2000

# Map topics to their SharePoint List Paths (extracted from user URLs)
CATEGORIES = {
    "Thời sự chính trị": "/ct/cms/tintuc/Lists/ThoiSuChinhTri",
    "Kinh tế": "/ct/cms/tintuc/Lists/KinhTe",
    "Văn hóa xã hội": "/ct/cms/tintuc/Lists/VanHoaXaHoi",
    "Người tốt việc tốt": "/ct/cms/tintuc/Lists/NguoiTotViecTot",
    "Kỷ niệm ngày truyền thống": "/ct/cms/tintuc/Lists/KyNiemNgayTruyenThong",
    "Vĩnh Phúc vào xuân": "/doanhnghiep/Lists/vinhphucvaoxuan",
    "Điểm tham quan": "/ct/cms/dukhach/Lists/im thm quan", # "im thm quan" decoded
    "Di tích danh thắng": "/ct/cms/dukhach/Lists/DiTichDanhThang"
}

# Custom Adapter for Legacy SSL
class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        ctx.check_hostname = False
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize, block=block,
            ssl_version=ssl.PROTOCOL_TLSv1_2, ssl_context=ctx
        )

def clean_html(raw_html):
    if not raw_html: return ""
    # Decode XML/HTML entities
    text = html.unescape(raw_html)
    # Remove tags using BS4 for safety
    soup = BeautifulSoup(text, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    # Clean whitespace
    return re.sub(r'\s+', ' ', text).strip()

def main():
    session = requests.Session()
    session.mount('https://', CustomHttpAdapter())
    
    # Load seen URLs/IDs if file exists
    seen_ids = set()
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Use URL as unique key, or verify Title if URL dynamic
                if "url" in row: seen_ids.add(row["url"])
    except: pass

    # Open CSV for writing
    write_header = not (seen_ids and len(seen_ids) > 0)
    mode = "a" if not write_header else "w"
    
    with open(OUTPUT_FILE, mode, encoding="utf-8-sig", newline="") as f:
        fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header: writer.writeheader()
        
        for topic, url_list_path in CATEGORIES.items():
            print(f"--- Crawling: {topic} ({url_list_path}) ---")
            
            start = 0
            total_fetched = 0
            
            while total_fetched < MAX_ITEMS_PER_CATEGORY:
                print(f"  Fetching from offset {start}...")
                payload = {
                    "do": "QUERYDATA",
                    "fieldOrder": "CreatedDate",
                    "ascending": "desc",
                    "UrlList": url_list_path,
                    "start": start,
                    "length": PAGE_SIZE
                }
                
                try:
                    resp = session.post(API_URL, data=payload, verify=False, timeout=20)
                    if resp.status_code != 200:
                        print(f"    Failed: {resp.status_code}")
                        break
                    
                    data_json = resp.json()
                    items = data_json.get("data", [])
                    
                    if not items:
                        print("    No more items.")
                        break
                        
                    for item in items:
                        # Construct URL
                        # Format: {ListPath}/View_Detail.aspx?ItemID={ID}
                        # Or if emagazine: /Pages/tintuc_emagazine.aspx...
                        # We'll use the standard view for now.
                        item_id = item.get("ID")
                        if not item_id: continue
                        
                        full_url = f"https://vinhphuc.gov.vn{url_list_path}/View_Detail.aspx?ItemID={item_id}"
                        
                        if full_url in seen_ids: continue
                        
                        # Extract Content
                        raw_content = item.get("ContentNews", "")
                        content = clean_html(raw_content)
                        
                        # Summary
                        summary = item.get("DescriptionNews", "")
                        if not summary:
                            # If no summary from API, take first 500 chars and try to cut at last punctuation
                            content_snippet = content[:500]
                            last_dot = max(content_snippet.rfind('.'), content_snippet.rfind('!'), content_snippet.rfind('?'))
                            if last_dot > 200: # Ensure we have a decent amount of text
                                summary = content_snippet[:last_dot + 1]
                            else:
                                summary = content_snippet
                            if len(content) > 500 and not summary.endswith('...'):
                                summary += "..."
                        
                        # Date
                        # Format: 2025-06-30T19:02:21
                        raw_date = item.get("CreatedDate", "")
                        
                        # Keywords (Keywords not always in API, check 'Keywords' field if exists, else empty)
                        # API response doesn't show 'Keywords' key in debug, maybe empty.
                        keywords = ""
                        
                        row = {
                            "topic": topic,
                            "title": item.get("Title", "").replace("##", "").strip(),
                            "summary": summary.strip(),
                            "url": full_url,
                            "keywords": keywords,
                            "public_time": raw_date,
                            "content": content
                        }
                        
                        writer.writerow(row)
                        seen_ids.add(full_url)
                        
                    f.flush()
                    
                    fetched_count = len(items)
                    total_fetched += fetched_count
                    start += fetched_count
                    
                    print(f"    Saved {fetched_count} items. Total: {total_fetched}")
                    
                    # If we got fewer items than requested, likely end of list
                    if fetched_count < PAGE_SIZE:
                        break
                        
                    time.sleep(1) # Polite delay
                    
                except Exception as e:
                    print(f"    Error scraping offset {start}: {e}")
                    time.sleep(5)
                    # Retry logic or break?
                    break
    
    print(f"\n--- Crawl Finished. Data saved to {OUTPUT_FILE} ---")

if __name__ == "__main__":
    main()
