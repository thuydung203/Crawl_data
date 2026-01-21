import requests
import csv
import re
import time
import html
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

# ================= CONFIG =================
BASE_URL = "https://thainguyen.gov.vn"
OUTPUT_FILE = "thainguyen_data_final.csv"
MAX_PAGES_PER_TOPIC = 50   # Adjust as needed (user has many categories)
SLEEP = 1

# List provided by user (normalized to full URLs if needed)
CATEGORIES = {
    "Tin hoạt động của lãnh đạo chính quyền": "https://thainguyen.gov.vn/vi_VN/tin-hoat-dong-cua-lanh-dao-chinh-quyen",
    "Đổi mới sáng tạo vì Thái Nguyên thân yêu": "https://thainguyen.gov.vn/vi_VN/doi-moi-sang-tao-vi-thai-nguyen-than-yeu",
    "Công nghệ": "https://thainguyen.gov.vn/vi_VN/cong-nghe",
    "Tin tức chuyển đổi số": "https://thainguyen.gov.vn/vi_VN/tin-tuc-chuyen-do-so",
    "Tin hoạt động doanh nghiệp": "https://thainguyen.gov.vn/vi_VN/tin-hoat-dong-doanh-nghiep/",
    "Tin tức bầu cử": "https://thainguyen.gov.vn/vi_VN/tin-tuc-bau-cu",
    "Đất và người Thái Nguyên": "https://thainguyen.gov.vn/vi_VN/dat-va-nguoi-thai-nguyen",
    "Cẩm nang du lịch": "https://thainguyen.gov.vn/vi_VN/cam-nang-du-lich",
    "Chương trình du lịch hấp dẫn": "https://thainguyen.gov.vn/vi_VN/chuong-trinh-du-lich-hap-dan",
    "Văn hóa": "https://thainguyen.gov.vn/van-hoa",
    "Thời sự": "https://thainguyen.gov.vn/vi_VN/thoi-su",
    "Tin hoạt động của tỉnh": "https://thainguyen.gov.vn/vi_VN/tin-hoat-dong-cua-tinh",
    "Tin hoạt động của đơn vị": "https://thainguyen.gov.vn/vi_VN/tin-hoat-dong-cua-don-vi",
    "Bài viết Thái Nguyên": "https://thainguyen.gov.vn/vi_VN/bai-viet-thai-nguyen",
    "Tin trong nước": "https://thainguyen.gov.vn/vi_VN/tin-trong-nuoc",
    "Tin quốc tế": "https://thainguyen.gov.vn/vi_VN/tin-quoc-te"
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
})

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def normalize_url(url):
    if not url:
        return None
    url = url.strip()
    if url.startswith("javascript:"):
        return None
    if url.startswith("http"):
        return url
    return urljoin(BASE_URL, url)

# ================= PAGINATION HELPER =================
def get_pagination_info(soup, current_url):
    """
    Finds the 'Next' or page links to determine the base URL and the 'cur' parameter name.
    Liferay usually uses a parameter like '_101_INSTANCE_CxpLMxKIhxrm_cur=2'.
    Returns: (base_url_no_query, params_dict, cur_param_key)
    """
    # Look for 'Tiếp' or 'Next' or page numbers
    candidates = soup.find_all("a", href=True)
    
    pagination_link = None
    for a in candidates:
        txt = a.get_text().strip().lower()
        if txt in ["tiếp", "next", "sau", ">"] or re.match(r"^\d+$", txt):
            # Check if href has 'cur' parameter
            if "cur=" in a["href"]:
                pagination_link = a["href"]
                break
    
    if not pagination_link:
        return None, None, None

    full_url = normalize_url(pagination_link)
    parsed = urlparse(full_url)
    params = parse_qs(parsed.query)
    
    # Identify the 'cur' key (e.g. '_101_INSTANCE_..._cur')
    cur_key = next((k for k in params.keys() if k.endswith("_cur") or k == "cur"), None)
    
    # If found, prepare base params
    if cur_key:
        # Convert list to single value
        flat_params = {k: v[0] for k, v in params.items()}
        return full_url.split("?")[0], flat_params, cur_key
        
    return None, None, None

# ================= PARSE ARTICLE =================
def parse_article(url, topic):
    result = {
        "topic": topic,
        "title": "",
        "summary": "",
        "url": url,
        "keywords": "",
        "public_time": "",
        "content": ""
    }
    
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # TITLE
        # Try finding generic title classes often used in Liferay or this site
        title_candidates = [
            soup.find("h1", class_="title-detail"),
            soup.find("h1", class_="article-title"),
            soup.find(class_="title-art"),
            soup.find("h3", class_="header-title"),
        ]
        
        for t in title_candidates:
            if t:
                txt = clean_text(t.get_text())
                if len(txt) > 5 and "CỔNG THÔNG TIN" not in txt.upper():
                    result["title"] = txt
                    break
        
        # Fallback: Find all h1s and pick the longest one or one that isn't the site title
        if not result["title"]:
            for h1 in soup.find_all("h1"):
                txt = clean_text(h1.get_text())
                if len(txt) > 10 and "CỔNG THÔNG TIN" not in txt.upper():
                    result["title"] = txt
                    break

        if not result["title"] and soup.title:
             t_text = clean_text(soup.title.string)
             result["title"] = t_text.split("- Cổng")[0].strip()

        # PUBLIC TIME
        # Look for dates
        # Support dd/mm/yyyy, yyyy-mm-dd, and with HH:MM:SS
        date_pattern = re.compile(r"(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{4}-\d{1,2}-\d{1,2})(?:\s*[,|-]?\s*\d{1,2}:\d{1,2}(?::\d{1,2}(?:\.\d+)?)?)?")
        
        # Avoid site clock (usually in class date-today or similar)
        blacklist_classes = ["date-today", "current-date", "clock"]
        
        # 1. Search specific classes
        time_tags = soup.find_all(class_=re.compile("date|time|publish|created"))
        for tag in time_tags:
            # Check blacklist
            classes = tag.get("class", [])
            if any(b in str(c) for b in blacklist_classes for c in classes):
                continue
                
            match = date_pattern.search(tag.get_text())
            if match:
                result["public_time"] = match.group(0).strip()
                break
        
        # 2. Fallback: Search in small text nodes near title (h1)
        if not result["public_time"]:
            # Sometimes date is a text node right after H1 or in the same container
            for h1 in soup.find_all("h1"):
                # Check siblings
                for sib in h1.next_siblings:
                    if sib.name == "div" or sib.name == "span" or isinstance(sib, str):
                        txt = str(sib if isinstance(sib, str) else sib.get_text())
                        match = date_pattern.search(txt)
                        if match:
                             result["public_time"] = match.group(0).strip()
                             break
                if result["public_time"]: break

        # 3. Fallback: Search full text (dangerous but necessary)
        if not result["public_time"]:
            text_content = soup.get_text(" ", strip=True)
            # Limit search to first 3000 chars to avoid footer dates
            match = date_pattern.search(text_content[:3000])
            if match:
                 result["public_time"] = match.group(0).strip()

        # SUMMARY
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            result["summary"] = clean_text(meta_desc.get("content"))
        
        if not result["summary"]:
            sapo = soup.find(class_=re.compile("sapo|summary"))
            if sapo:
                 result["summary"] = clean_text(sapo.get_text())

        # CONTENT
        # Liferay content is often in class="journal-content-article" or specific IDs
        content_div = soup.find(class_="journal-content-article") or \
                      soup.find(class_="content-detail") or \
                      soup.find(id="main-content")
                      
        if content_div:
            # Cleanup
            for tag in content_div(["script", "style", "iframe", "form", "div"]): 
                 # Some divs are just wrappers, but often garbage in news sites
                 if "related" in str(tag.get("class", "")) or "comment" in str(tag.get("class", "")):
                     tag.decompose()
            
            paragraphs = []
            for p in content_div.find_all(["p", "div"]):
                 txt = clean_text(p.get_text())
                 # Remove specific garbage
                 if "Your browser does not support" in txt:
                     continue
                 if len(txt) > 20:
                     paragraphs.append(txt)
            
            # Flatten to avoiding breaking CSV lines visually if not fully supported
            result["content"] = " ".join(paragraphs)

        # KEYWORDS
        meta_kw = soup.find("meta", attrs={"name": "keywords"})
        if meta_kw:
            result["keywords"] = clean_text(meta_kw.get("content"))

        return result

    except Exception as e:
        print(f"Error parsing {url}: {e}")
        return None

# ================= MAIN =================
def main():
    seen_urls = set()
    
    # Always start fresh? NO, now we want RESUME capability
    mode = "w"
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("url"):
                        seen_urls.add(row["url"])
            print(f"RESUMING: Loaded {len(seen_urls)} existing articles.")
            mode = "a"
        except Exception as e:
            print(f"Error reading existing file: {e}. Starting fresh.")
            mode = "w"

    # Setup file
    with open(OUTPUT_FILE, mode, newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, 
            fieldnames=["topic", "title", "summary", "url", "keywords", "public_time", "content"]
        )
        
        # Only write header if starting fresh
        if mode == "w":
            writer.writeheader()
            
        for topic, category_url in CATEGORIES.items():
            category_url = normalize_url(category_url)
            print(f"\n=== Processing: {topic} ===")
            
            # 1. Get first page and pagination info
            try:
                resp = session.get(category_url, timeout=30)
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # Extract links from page 1
                links_found = 0
                
                # Main content area assumption: Liferay generic
                main_area = soup.find("div", class_="portlet-asset-publisher") or soup.body
                
                # Find asset links
                current_page_links = []
                for a in main_area.find_all("a", href=True):
                    href = a["href"]
                    # Usually articles have /-/asset_publisher/ or contain document ID
                    if "/-/asset_publisher/" in href or "content" in href:
                        full_link = normalize_url(href)
                        if full_link and full_link not in seen_urls:
                             current_page_links.append(full_link)
                
                # Dedupe
                current_page_links = list(dict.fromkeys(current_page_links))
                print(f"  Page 1: Found {len(current_page_links)} new articles.")
                
                for link in current_page_links:
                    seen_urls.add(link)
                    data = parse_article(link, topic)  # Passed topic name
                    if data and data["title"]:
                        writer.writerow(data)
                        links_found += 1
                        print(f"    Saved: {data['title'][:40]}...")
                        f.flush()
                    time.sleep(SLEEP)
                    
                # Setup pagination
                base_url, base_params, cur_key = get_pagination_info(soup, category_url)
                
                if not base_url or not cur_key:
                    print("  No pagination detected or single page.")
                    continue
                    
                print(f"  Pagination: {base_url} | Param: {cur_key}")
                
                # Iterate
                for page in range(2, MAX_PAGES_PER_TOPIC + 1):
                    base_params[cur_key] = str(page)
                    print(f"  -> Page {page}...")
                    
                    try:
                        p_resp = session.get(base_url, params=base_params, timeout=30)
                        p_soup = BeautifulSoup(p_resp.text, "html.parser")
                        
                        # Extract links
                        main_area = p_soup.find("div", class_="portlet-asset-publisher") or p_soup.body
                        page_links = []
                        for a in main_area.find_all("a", href=True):
                            href = a["href"]
                            if "/-/asset_publisher/" in href or "content" in href:
                                full_link = normalize_url(href)
                                if full_link and full_link not in seen_urls:
                                     page_links.append(full_link)
                                     
                        page_links = list(dict.fromkeys(page_links))
                        if not page_links:
                            print("    No new links. Stopping category.")
                            break
                            
                        print(f"    Found {len(page_links)} new articles.")
                        for link in page_links:
                            seen_urls.add(link)
                            data = parse_article(link, topic)  # Passed topic name
                            if data and data["title"]:
                                writer.writerow(data)
                                print(f"      Saved: {data['title'][:40]}...")
                                f.flush()
                            time.sleep(SLEEP)
                            
                    except Exception as e:
                        print(f"    Error page {page}: {e}")
            
            except Exception as e:
                print(f"  Error category {category_url}: {e}")

if __name__ == "__main__":
    main()
