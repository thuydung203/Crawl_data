import requests
import csv
import re
import time
import html
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Tắt cảnh báo SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= CONFIG SƠN LA =================
BASE_URL = "https://sonla.gov.vn"
OUTPUT_FILE = "sonla_data_final.csv"
MAX_PAGES_PER_TOPIC = 5000 
SLEEP = 0.5

CATEGORIES = {
    "Chính trị": "https://sonla.gov.vn/tin-chinh-tri",
    "Kinh tế": "https://sonla.gov.vn/tin-kinh-te",
    "Văn hóa - Xã hội": "https://sonla.gov.vn/tin-van-hoa-xa-hoi",
    "An ninh - Quốc phòng": "https://sonla.gov.vn/an-ninh-quoc-phong",
    "Doanh nghiệp": "https://sonla.gov.vn/chinh-quyen-voi-doanh-nghiep",
    "Lịch sử": "https://sonla.gov.vn/lich-su-son-la",
    "Điều kiện tự nhiên": "https://sonla.gov.vn/dieu-kien-tu-nhien",
    "Xã phường": "https://sonla.gov.vn/cac-xa-phuong",
    "Cơ sở hạ tầng": "https://sonla.gov.vn/co-so-ha-tang",
    "Di sản văn hóa": "https://sonla.gov.vn/di-san-van-hoa",
    "Dân tộc": "https://sonla.gov.vn/cac-dan-toc-son-la",
    "Đối ngoại": "https://sonla.gov.vn/doi-ngoai-nhan-dan"
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
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

# ================= PARSE CHI TIẾT BÀI VIẾT =================
# ================= PARSE CHI TIẾT BÀI VIẾT =================
def parse_article(url, topic):
    result = {
        "topic": topic, "title": "", "summary": "", "url": url,
        "keywords": "", "public_time": "", "content": ""
    }
    try:
        resp = session.get(url, timeout=20, verify=False)
        if resp.status_code != 200: return None
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # 1. Tiêu đề - Prioritize .ArticleHeader
        title_tag = soup.select_one(".ArticleHeader, .title-news, h1")
        if title_tag:
            result["title"] = clean_text(title_tag.get_text())
        elif soup.find("meta", attrs={"name": "title"}):
             result["title"] = clean_text(soup.find("meta", attrs={"name": "title"}).get("content"))

        # 2. Thời gian
        time_tag = soup.select_one(".PostDate, .date, .time, .cms-date")
        if time_tag: result["public_time"] = clean_text(time_tag.get_text())

        # 3. TÓM TẮT - Prioritize .ArticleSummary over meta description
        summary_tag = soup.select_one(".ArticleSummary, .summary, .sapo")
        if summary_tag: 
            result["summary"] = clean_text(summary_tag.get_text())
        
        # Fallback to meta description ONLY if summary tag is missing or empty
        if not result["summary"]:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc: 
                desc_content = clean_text(meta_desc.get("content"))
                # Filter out generic site descriptions
                if "Cổng thông tin điện tử" not in desc_content:
                    result["summary"] = desc_content
        
        # 4. KEYWORDS
        meta_kw = soup.find("meta", attrs={"name": "keywords"})
        if meta_kw: result["keywords"] = clean_text(meta_kw.get("content"))

        # 5. Nội dung
        content_div = soup.select_one(".ArticleContent, .journal-content-article, #content")
        if content_div:
            for trash in content_div.select("script, style, .social-share, .tags, .rating, .tool, .related-news"):
                trash.decompose()
            result["content"] = clean_text(content_div.get_text())

        return result
    except Exception:
        return None

# ================= LẤY LINK BÀI VIẾT =================
def extract_article_links(soup, seen_urls):
    links = []
    # Broad search for news containers
    # Added .ArticleInMenu, .ArticleList based on debug HTML
    containers = soup.select(".ArticleInMenu, .ArticleList, .portlet-asset-publisher, .list-news, #main-content, .Content-Body, .ModuleContent, .news-list, .post-list, .item-list, .content-list, .view-content, .category-view")
    
    if not containers:
         containers = [soup]

    for container in containers:
        # Prioritize title links first
        title_links = container.select(".Title a, h2 a, h3 a, .title-news a")
        for a in title_links:
             href = a.get("href")
             if href:
                full_url = normalize_url(href)
                if full_url and full_url not in seen_urls and "sonla.gov.vn" in full_url:
                    # Basic filter
                    if any(x in href for x in ["/admin/", "/login", "/search", "mailto:", "tel:", "format=pdf", "Default.aspx", "pageid="]): continue
                    links.append(full_url)

        # Then finding all links if specific title links missed some
        for a in container.find_all("a", href=True):
            href = a["href"]
            # Filter for article-like links
            if ("/" in href and len(href) > 20 and not href.startswith("javascript") and not href.startswith("#") and ".jpg" not in href and ".png" not in href):
                 # Exclude system pages and non-articles
                if any(x in href for x in ["/admin/", "/login", "/search", "mailto:", "tel:", "dailoan", "format=pdf", "Default.aspx", "pageid="]): continue
                
                full_url = normalize_url(href)
                if full_url and full_url not in seen_urls and "sonla.gov.vn" in full_url:
                    links.append(full_url)
    
    unique_links = list(dict.fromkeys(links))
    print(f"    DEBUG: Found {len(unique_links)} potential links.")
    return unique_links

# ================= HÀM MAIN VỚI CƠ CHẾ PHÂN TRANG =================
def main():
    seen_urls = set()
    print("--- BẮT ĐẦU CÀO SƠN LA ---")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["topic", "title", "summary", "url", "keywords", "public_time", "content"])
        writer.writeheader()

        for topic, start_url in CATEGORIES.items():
            print(f"\n[MỤC]: {topic}")
            current_page = 1
            target_url = start_url
            
            while current_page <= MAX_PAGES_PER_TOPIC:
                print(f"  > Đang xử lý Trang {current_page}: {target_url}", end="\r")
                
                try:
                    resp = session.get(target_url, timeout=30, verify=False)
                    soup = BeautifulSoup(resp.text, "html.parser")
                    
                    # 1. Lấy link bài viết
                    links = extract_article_links(soup, seen_urls)
                    
                    if not links:
                        print(f"\n  ! Không thấy bài viết mới ở trang {current_page}. Thử tìm trang tiếp...", flush=True)
                        with open("debug_empty_page_v2.html", "w", encoding="utf-8") as f:
                            f.write(resp.text)
                        print(f"    DEBUG: Saved HTML of empty page to debug_empty_page_v2.html", flush=True)
                    
                    # 2. Cào từng bài
                    count_in_page = 0
                    for link in links:
                        seen_urls.add(link)
                        data = parse_article(link, topic)
                        if data and data["title"]: # Basic validation
                            writer.writerow(data)
                            count_in_page += 1
                        time.sleep(SLEEP)
                    
                    print(f"\n  v Hoàn thành trang {current_page} (Lấy được {count_in_page} bài).")

                    # 3. KIỂM TRA NÚT TRANG TIẾP THEO
                    next_url = None
                    
                    # Tìm container phân trang broad hơn
                    pagination = soup.select_one(".pagination, .pager, .lfr-pagination-buttons, .taglib-page-iterator, .ui-pagination, .pages, .pagination-container, ul.pagination, nav ul")
                    
                    if pagination:
                        # 1. Tìm theo class 'next' hoặc 'Next'
                        next_a = pagination.select_one(".next a, a.next, .Next a, li.next a, li.Next a, a[rel='next'], a[aria-label='Next'], a.page-link[aria-label='Next']")
                        if next_a:
                            next_url = next_a.get("href")
                        
                        # 2. Tìm theo text "Sau", "Tiếp", "Next", ">" nhưng phải nằm trong container này
                        if not next_url:
                            for a in pagination.find_all("a", href=True):
                                text = clean_text(a.get_text())
                                if re.search(r"^(Trang sau|Sau|Tiếp|Next|>|»)$", text, re.I):
                                    next_url = a.get("href")
                                    # Kiểm tra nếu là nút disable
                                    if "disabled" in a.get("class", []) or "disabled" in a.find_parent("li", {}).get("class", []):
                                         next_url = None
                                    break

                    if not next_url:
                        # Fallback: Tìm a có class 'next' ở toàn trang (nhưng không tìm text "Tiếp" bừa bãi)
                        next_a = soup.select_one("a.next, .next a, a[rel='next']")
                        if next_a: next_url = next_a.get("href")

                    if next_url and "javascript" not in next_url:
                        target_url = normalize_url(next_url)
                        if target_url == resp.url or target_url in seen_urls: # Check loop
                             print(f"  x Link trang tiếp trùng trang hiện tại hoặc đã cào. Dừng.", flush=True)
                             break
                        
                        current_page += 1
                        # print(f"    DEBUG: Next URL: {target_url}", flush=True)
                    else:
                        print(f"  x Không thấy nút trang tiếp hoặc link js. Kết thúc mục {topic}.\n", flush=True)
                        break
                        
                except Exception as e:
                    print(f"\n  ! Lỗi tại trang {current_page}: {e}", flush=True)
                    import traceback
                    traceback.print_exc()
                    break

    print(f"\n--- HOÀN THÀNH! Dữ liệu tại: {OUTPUT_FILE} ---")

if __name__ == "__main__":
    main()
