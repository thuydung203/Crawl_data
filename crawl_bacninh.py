import requests
import urllib3
import ssl
import csv
import time
import re
import html
from bs4 import BeautifulSoup
import logging

# Check for lxml, fallback to html.parser if not present
try:
    import lxml
    PARSER = 'lxml'
except ImportError:
    PARSER = 'html.parser'

urllib3.disable_warnings()

# Configuration
OUTPUT_FILE = "bacninh_data_final.csv"
PAGE_SIZE = 50 
MAX_ITEMS_PER_TOPIC = 2000

# Topics and their URLs
TOPICS = {
    "Chính trị": "https://bacninh.gov.vn/chinh-tri",
    "Kinh tế": "https://bacninh.gov.vn/kinh-te",
    "Văn hóa": "https://bacninh.gov.vn/van-hoa",
    "Lịch sử văn hóa": "https://bacninh.gov.vn/lich-su-van-hoa",
    "Xã hội": "https://bacninh.gov.vn/xa-hoi",
    "Sở ngành địa phương": "https://bacninh.gov.vn/so-nganh-dia-phuong",
    "Tin trong nước": "https://bacninh.gov.vn/tin-trong-nuoc",
    "Tin quốc tế": "https://bacninh.gov.vn/tin-quoc-te",
    "Du khách": "https://bacninh.gov.vn/du-khach",
    "Tiềm năng phát triển": "https://bacninh.gov.vn/tiem-nang-phat-trien",
    "Cơ sở hạ tầng": "https://bacninh.gov.vn/co-so-ha-tang"
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
    text = html.unescape(raw_html)
    soup = BeautifulSoup(text, PARSER)
    
    caption_keywords = ["Ảnh minh họa", "nguồn Internet", "Ảnh:", "Nguồn:", "(Ảnh:"]
    for tag in soup.find_all(['span', 'i', 'em', 'figcaption', 'p']):
        txt = tag.get_text(strip=True)
        if len(txt) < 200 and any(kw in txt for kw in caption_keywords):
            if "Ảnh minh họa" in txt or "nguồn Internet" in txt:
                tag.decompose()
            elif re.search(r'^(Ảnh|Nguồn):', txt, re.I):
                tag.decompose()

    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r'\s+', ' ', text).strip()

def extract_api_url(session, page_url):
    try:
        resp = session.get(page_url, verify=False, timeout=30)
        match = re.search(r"loadPageURL\s*[:=]\s*['\"]([^'\"]+)['\"]", resp.text)
        if match: return match.group(1)
        candidates = re.findall(r"['\"](https?://[^'\"]+p_p_resource_id=loadPage[^'\"]*)['\"]", resp.text)
        if candidates: return candidates[0]
        return None
    except: return None

def main():
    session = requests.Session()
    session.mount('https://', CustomHttpAdapter())
    
    seen_ids = set()
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "url" in row: seen_ids.add(row["url"])
        print(f"Loaded {len(seen_ids)} existing articles.")
    except: pass

    exists = os.path.exists(OUTPUT_FILE)
    mode = 'a' if exists else 'w'
    
    with open(OUTPUT_FILE, mode, encoding="utf-8-sig", newline="") as f:
        fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists: writer.writeheader()
        
        for topic_name, topic_url in TOPICS.items():
            print(f"--- Processing Topic: {topic_name} ---")
            api_url = extract_api_url(session, topic_url)
            if not api_url: continue
                
            page = 1
            total_fetched_topic = 0
            while total_fetched_topic < MAX_ITEMS_PER_TOPIC:
                params = {'pageNum': page, 'recordPerPage': PAGE_SIZE, 'keyword': ''}
                try:
                    resp = session.get(api_url, params=params, verify=False, timeout=30)
                    if resp.status_code != 200: break
                    data = resp.json()
                    items = data.get('items', [])
                    total_pages = data.get('totalPageNum', 0)
                    if not items: break
                        
                    count_new = 0
                    for item in items:
                        url_detail = item.get('urlDetail', '')
                        if not url_detail:
                            item_id = item.get('id')
                            if item_id:
                                url_detail = f"{topic_url}?p_p_id=newsbycategory_WAR_bacninhportlet&p_p_lifecycle=0&_newsbycategory_WAR_bacninhportlet_articleId={item_id}"
                            else: continue
                        
                        if url_detail in seen_ids: continue
                            
                        # Fetch full content from detail page to avoid truncation
                        print(f"    - Fetching: {url_detail}")
                        try:
                            resp_d = session.get(url_detail, verify=False, timeout=20)
                            if resp_d.status_code == 200:
                                s_d = BeautifulSoup(resp_d.text, PARSER)
                                t_tag = s_d.select_one('h1#contentDetailTitleId')
                                s_tag = s_d.select_one('div#sapoDetailId')
                                c_tag = s_d.select_one('div#contentDetail')
                                
                                # Fallbacks
                                if not t_tag: t_tag = s_d.select_one('.title-detail, .news-title, h1')
                                if not s_tag: s_tag = s_d.select_one('.summary, .lead, .sapo')
                                if not c_tag: c_tag = s_d.select_one('.content-detail, .news-content, .detail-content')

                                title = t_tag.get_text(strip=True) if t_tag else item.get('title', '').strip()
                                # Prevent "Thực đơn" as title
                                if title == "Thực đơn" and item.get('title'):
                                    title = item.get('title').strip()

                                summary = clean_html(str(s_tag)) if s_tag else clean_html(item.get('summary', ''))
                                content = clean_html(str(c_tag)) if c_tag else clean_html(item.get('content', ''))
                                
                                writer.writerow({
                                    "topic": topic_name,
                                    "title": title,
                                    "summary": summary,
                                    "url": url_detail,
                                    "keywords": "",
                                    "public_time": item.get('createdDate', ''),
                                    "content": content
                                })
                                seen_ids.add(url_detail)
                                count_new += 1
                        except: continue
                        
                    f.flush()
                    total_fetched_topic += count_new
                    if page >= total_pages: break
                    page += 1
                    time.sleep(0.5)
                except: break
    print("\nCrawl Complete.")

if __name__ == "__main__":
    main()
