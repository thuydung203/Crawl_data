import requests
import csv
import re
import time
import html
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= CONFIG ĐIỆN BIÊN =================
BASE_URL = "https://www.dienbien.gov.vn"
OUTPUT_FILE = "dienbien_data_final.csv"
MAX_PAGES = 500 
SLEEP = 0.5

CATEGORIES = {
    "Hoạt động lãnh đạo UBND": "https://www.dienbien.gov.vn/portal/Pages/Hoat-dong-lanh-dao-UBND-tinh.aspx",
    "Cơ chế chính sách": "https://dienbien.gov.vn/portal/Pages/co_che_chinh_sach.aspx",
    "Du lịch - Điểm tham quan": "https://dienbien.gov.vn/portal/Pages/Diem-tham-quan.aspx",
    "Du lịch - Viết về Điện Biên": "https://dienbien.gov.vn/portal/Pages/Viet-ve-du-lich-Dien-Bien.aspx",
    "Phòng chống tham nhũng": "https://dienbien.gov.vn/portal/Pages/Tuyen-truyen-phong-chong-tham-nhung.aspx",
    "Thông tin vụ án tham nhũng": "https://dienbien.gov.vn/portal/Pages/Thong-tin-vu-an-vu-viec-tham-nhung.aspx"
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

def clean_text(s):
    if not s: return ""
    s = html.unescape(s)
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def parse_article(url, topic):
    try:
        resp = session.get(url, timeout=25, verify=False)
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # 1. Title
        title = ""
        title_tag = soup.select_one(".tandan-p-article-news-title")
        if title_tag:
            title = clean_text(title_tag.get_text())
        else:
            if soup.title:
                title = soup.title.string.split(" - CỔNG THÔNG TIN")[0].strip()
        
        # 2. Summary
        summary = ""
        summary_tag = soup.select_one(".tandan-p-article-news-summary")
        if summary_tag:
            summary = clean_text(summary_tag.get_text())
        else:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc: summary = clean_text(meta_desc.get("content"))

        # 3. Keywords (Tags)
        keywords = ""
        # Check for tags in .td-tags
        tag_links = soup.select(".td-tags a")
        if tag_links:
            kw_list = [clean_text(t.get_text()) for t in tag_links if t.get_text() and "Tags" not in t.get_text()]
            keywords = ", ".join(kw_list)
        
        if not keywords:
            meta_kw = soup.find("meta", attrs={"name": "keywords"})
            if meta_kw:
                keywords = clean_text(meta_kw.get("content"))

        # 4. Public Time
        public_time = ""
        time_tag = soup.select_one(".tandan-span-date-publish, .date, .time, .publish-date")
        if time_tag:
            public_time = clean_text(time_tag.get_text())
            # Clean "Đăng ngày " prefix if present
            public_time = public_time.replace("Đăng ngày", "").strip()

        # 5. Content
        content = ""
        # .tandan-p-article-main is the container used in the site
        content_div = soup.select_one(".tandan-p-article-main, .ms-rtestate-field, .tandan-p-article-news-content, #content")
        
        if content_div:
            # Remove unrelated elements
            for trash in content_div.select("script, style, .social-share, .tandan-div-article-other, #ctl00_PlaceHolderMain_ctl08_label"):
                trash.decompose()
            content = clean_text(content_div.get_text())
        
        # Fallback
        if not content or len(content) < 50:
             main_area = soup.select_one("#main-content, .ms-webpart-zone, .ms-rtestate-write")
             if main_area: content = clean_text(main_area.get_text())

        # Validation: Skip if no content
        if not content or len(content) < 20:
            return None

        return {
            "topic": topic, "title": title, "summary": summary, "url": url,
            "keywords": keywords, "public_time": public_time, "content": content
        }
    except Exception as e:
        # print(f"Error parsing {url}: {e}")
        return None

def main():
    seen_urls = set()
    print("--- BẮT ĐẦU CÀO ĐIỆN BIÊN: ĐÚNG CHỦ ĐỀ YÊU CẦU ---")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["topic", "title", "summary", "url", "keywords", "public_time", "content"])
        writer.writeheader()
        f.flush()

        for topic, start_url in CATEGORIES.items():
            print(f"\n[MỤC]: {topic}")
            
            # Cào cả trang gốc nếu nó chứa nội dung trực tiếp
            root_data = parse_article(start_url, topic)
            if root_data and len(root_data["content"]) > 150:
                writer.writerow(root_data)
                f.flush()

            for page in range(1, MAX_PAGES + 1):
                # Sử dụng PageIndex để phân trang cho SharePoint
                target_url = start_url if page == 1 else f"{start_url}?PageIndex={page}"
                print(f"  > Trang {page}/{MAX_PAGES}...", end="\r")
                
                try:
                    resp = session.get(target_url, timeout=30, verify=False)
                    soup = BeautifulSoup(resp.text, "html.parser")
                    
                    links = []
                    for a in soup.find_all("a", href=True):
                        href = a['href']
                        if "/portal/Pages/" in href and ".aspx" in href:
                            # Filter unwanted paths
                            lower_href = href.lower()
                            if any(x in lower_href for x in ["default.aspx", "login.aspx", "/home-new/", "lich-tiep-cong-dan"]): 
                                continue
                            
                            full_url = urljoin(BASE_URL, href)
                            # Không cào lại trang gốc và link đã thấy
                            if full_url not in seen_urls and full_url != start_url and full_url not in links:
                                links.append(full_url)
                    
                    if not links and page > 1: break
                    
                    for link in links:
                        if link in seen_urls: continue # Double check
                        seen_urls.add(link)
                        data = parse_article(link, topic)
                        if data and data["title"]:
                            writer.writerow(data)
                            f.flush()
                        time.sleep(SLEEP)
                    
                except Exception:
                    break

    print(f"\n--- XONG! Kiểm tra file: {OUTPUT_FILE} ---")

if __name__ == "__main__":
    main()
