import requests
import csv
import re
import time
import html
import urllib3
from bs4 import BeautifulSoup
import ssl
from urllib.parse import urljoin

# Tắt cảnh báo SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= CONFIG BẮC GIANG =================
BASE_URL = "https://bacgiang.gov.vn"
OUTPUT_FILE = "bacgiang_data_final_v2.csv"
MAX_PAGES_PER_TOPIC = 50 
SLEEP = 0.5 

CATEGORIES = {
    "Chính trị": "https://bacgiang.gov.vn/chinh-tri",
    "Kinh tế": "https://bacgiang.gov.vn/kinh-te",
    "Văn hóa - Xã hội": "https://bacgiang.gov.vn/van-hoa-xa-hoi",
    "Quốc phòng - An ninh": "https://bacgiang.gov.vn/quoc-phong-an-ninh",
    "Sở ban ngành": "https://bacgiang.gov.vn/so-ban-nganh",
    "Hội đoàn thể": "https://bacgiang.gov.vn/hoi-doan-the",
    "Huyện - Thành phố": "https://bacgiang.gov.vn/huyen-thanh-pho",
    "Tin trong nước": "https://bacgiang.gov.vn/tin-trong-nuoc",
    "Tin quốc tế": "https://bacgiang.gov.vn/tin-tuc-quoc-te"
}

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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

def clean_text(s: str) -> str:
    if not s: return ""
    s = html.unescape(s)
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def normalize_url(url):
    if not url: return None
    url = url.strip()
    if url.startswith("http"): return url
    return urljoin(BASE_URL, url)

# ================= PARSE CHI TIẾT BÀI VIẾT (FIX TITLE & CLEAN) =================
def parse_article(url, topic):
    try:
        resp = session.get(url, timeout=25, verify=False)
        if resp.status_code != 200: return None
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # 1. Title (Lấy từ h1 bên trong div.title-news hoặc h1 không có class rác)
        title_tag = soup.select_one(".title-news h1, .title-detail h1")
        if not title_tag:
            # Fallback: tìm h1 mà không phải portlet-title
            for h1 in soup.find_all("h1"):
                if "portlet-title" not in (h1.get("class") or []):
                    title_tag = h1
                    break
        
        title = clean_text(title_tag.get_text()) if title_tag else ""
        
        if not title and soup.title:
            title = soup.title.string.split(" - ")[0].strip()
            if "Cổng thông tin" in title:
                title = title.replace("Cổng thông tin điện tử tỉnh Bắc Giang", "").strip()

        # 2. Public Time (Lọc bỏ Lượt xem)
        public_time = ""
        time_tag = soup.find(class_=re.compile("date|time|publish|ngay-dang", re.I))
        if time_tag:
            raw_time = clean_text(time_tag.get_text())
            public_time = re.split(r'[|]|Lượt xem', raw_time, flags=re.IGNORECASE)[0].strip()

        # 3. Summary (Từ meta description)
        summary = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc: 
            summary = clean_text(meta_desc.get("content"))
        
        # 4. Keywords
        keywords = ""
        meta_kw = soup.find("meta", attrs={"name": "keywords"})
        if meta_kw: 
            keywords = clean_text(meta_kw.get("content"))

        # 5. Content (Lọc bỏ Ảnh, Caption, Tác giả, Nguồn)
        content = ""
        content_div = soup.select_one(".journal-content-article, .content-detail, .detail-content, .it-content")
        if content_div:
            # Loại bỏ script, style, img, iframe và các class rác cơ bản
            for trash in content_div.select("script, style, img, iframe, .social-share, .tags, .rating, .caption, .image-caption, .img-caption, .note"):
                trash.decompose()
            
            # Loại bỏ các tag chứa từ khóa caption (Ảnh minh họa, Nguồn, ...)
            caption_keywords = ["Ảnh minh họa", "nguồn Internet", "Ảnh:", "Nguồn:", "(Ảnh:"]
            for tag in content_div.find_all(['span', 'i', 'em', 'p', 'figcaption']):
                txt = tag.get_text(strip=True)
                if len(txt) < 200 and any(kw in txt for kw in caption_keywords):
                    tag.decompose()

            raw_content = content_div.get_text(separator=' ', strip=True)
            
            # Cắt bỏ phần Tác giả/Nguồn/Ảnh ở cuối bài bằng Regex
            raw_content = re.split(r'Tác giả:|Nguồn:|Tin, ảnh:|Ảnh:|Theo\s|Đọc thêm:|BGP|PV', raw_content, flags=re.IGNORECASE)[0]
            
            # Xử lý triệt để các pattern còn sót lại trong text
            raw_content = re.sub(r'Ảnh minh họa\s*\(nguồn\s*Internet\)\.?\s*', '', raw_content, flags=re.I)
            raw_content = re.sub(r'Ảnh minh họa\.?\s*', '', raw_content, flags=re.I)
            raw_content = re.sub(r'\(?nguồn\s*Internet\)?\.?\s*', '', raw_content, flags=re.I)
            
            content = clean_text(raw_content)

        return {
            "topic": topic, "title": title, "summary": summary, "url": url,
            "keywords": keywords, "public_time": public_time, "content": content
        }
    except Exception:
        return None

def extract_article_links(soup, seen_urls):
    links = []
    container = soup.select_one(".portlet-asset-publisher, .list-news, #main-content")
    if not container: container = soup
    for a in container.find_all("a", href=True):
        href = a["href"]
        if "/web/guest/-/" in href or "/-/asset_publisher/" in href:
            full_url = normalize_url(href)
            if full_url and full_url not in seen_urls and "bacgiang.gov.vn" in full_url:
                links.append(full_url)
    return list(dict.fromkeys(links))

def main():
    seen_urls = set()
    print("--- ĐANG CÀO BẮC GIANG: ĐÃ FIX TITLE & LÀM SẠCH NỘI DUNG ---")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["topic", "title", "summary", "url", "keywords", "public_time", "content"])
        writer.writeheader()

        for topic, start_url in CATEGORIES.items():
            print(f"\n[MỤC]: {topic}")
            current_page = 1
            target_url = start_url
            while current_page <= MAX_PAGES_PER_TOPIC:
                print(f"  > Quét Trang {current_page}...", end="\r")
                try:
                    resp = session.get(target_url, timeout=30, verify=False)
                    soup = BeautifulSoup(resp.text, "html.parser")
                    links = extract_article_links(soup, seen_urls)
                    if not links: break
                    for link in links:
                        seen_urls.add(link)
                        data = parse_article(link, topic)
                        if data and data["title"] and len(data["content"]) > 30:
                            writer.writerow(data)
                        time.sleep(SLEEP)
                    f.flush()
                    next_url = None
                    pagination = soup.select_one(".pagination, .pager, .lfr-pagination-buttons")
                    if pagination:
                        next_a = pagination.find("a", string=re.compile(r"Sau|Tiếp|Next|>", re.I))
                        if next_a: next_url = next_a.get("href")
                    if next_url and "javascript" not in next_url:
                        target_url = normalize_url(next_url)
                        current_page += 1
                    else: break
                except Exception: break

    print(f"\n--- XONG! Kiểm tra file: {OUTPUT_FILE} ---")

if __name__ == "__main__":
    main()