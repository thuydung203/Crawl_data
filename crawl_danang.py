import requests
import csv
import re
import time
import html
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

# ================= CONFIG =================
BASE_URL = "https://danang.gov.vn"
OUTPUT_FILE = "danang_data_final.csv"
MAX_PAGES_PER_TOPIC = 100 # Adjust as needed
SLEEP = 0.5

CATEGORIES = {
    "Lễ hội & Sự kiện": "https://danang.gov.vn/le-hoi-su-kien",
    "Nơi ở": "https://danang.gov.vn/noi-o",
    "Làng nghề truyền thống": "https://danang.gov.vn/lang-nghe-truyen-thong",
    "Tin tức - Sự kiện": "https://danang.gov.vn/tin-tuc-su-kien",
    "Chính quyền": "https://danang.gov.vn/vi/chinh-quyen",
    "Công dân": "https://danang.gov.vn/vi/cong-dan",
    "Doanh nghiệp": "https://danang.gov.vn/vi/doanh-nghiep",
    "Du khách": "https://danang.gov.vn/vi/du-khach"
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    # Remove control characters
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def normalize_url(url):
    if not url: 
        return None
    url = url.strip()
    if url.startswith("http"):
        return url
    return urljoin(BASE_URL, url)

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
        
        # TITLE REFINEMENT
        title_candidates = [
            soup.find(class_="title-detail"),
            soup.find("h1", class_="title-art"),
            soup.find("h1"),
            soup.find(class_="news-title")
        ]
        
        for t in title_candidates:
            if t:
                txt = clean_text(t.get_text())
                if len(txt) > 5 and "thực đơn" not in txt.lower():
                    result["title"] = txt
                    break
        
        if not result["title"] and soup.title:
            t_text = clean_text(soup.title.string)
            # Remove common suffixes
            t_text = re.sub(r"\s*-\s*Cổng thông tin.*$", "", t_text, flags=re.IGNORECASE)
            if "thực đơn" not in t_text.lower():
                 result["title"] = t_text

        # PUBLIC TIME
        result["public_time"] = ""
        # 1. Try common classes
        time_tag = soup.find(class_=re.compile("date|time|ngay-dang|publish-date|created-date|ngay_xb"))
        if time_tag:
            result["public_time"] = clean_text(time_tag.get_text())
        
        # 2. Regex fallback if empty
        if not result["public_time"]:
            # Look for dd/mm/yyyy hh:mm or similar
            date_pattern = re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{4}(?:\s*[,|-]?\s*\d{1,2}:\d{1,2})?")
            # Search in likely containers first to avoid false positives (like footer)
            for container in soup.find_all(["div", "span", "p"], class_=re.compile("meta|info|detail")):
                match = date_pattern.search(container.get_text())
                if match:
                    result["public_time"] = match.group(0).strip()
                    break
            
            # 3. Last resort: Search anywhere in body, but be careful (skip menus)
            if not result["public_time"]:
                # Exclude scripts and styles
                text_content = soup.get_text(" ", strip=True)
                match = date_pattern.search(text_content)
                if match:
                     result["public_time"] = match.group(0).strip()


        # SUMMARY
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            result["summary"] = clean_text(meta_desc.get("content"))
        
        if not result["summary"]:
            summary_div = soup.find(class_=re.compile("sapo|summary"))
            if summary_div:
                result["summary"] = clean_text(summary_div.get_text())

        # CONTENT
        content_div = soup.find(class_="journal-content-article") or \
                      soup.find(class_="content-detail") or \
                      soup.find(class_="view-content") or \
                      soup.find(id="main-content")
            
        if content_div:
            # Deep Cleanup
            for tag in content_div.find_all([
                "script", "style", "iframe", "form", "nav", "header", "footer", 
                "div", "section"
            ], class_=re.compile("portlet|metadata|tag-lib|social|rating|comment|related")):
                tag.decompose()
                
            paragraphs = []
            # We look for p tags or spans that contain significant text
            for p in content_div.find_all(["p", "div", "span"]):
                # Avoid nested duplicates
                if p.name in ["div", "span"] and p.find("p"):
                    continue
                    
                txt = clean_text(p.get_text())
                
                # Stop if we hit footer-like indicators in the text
                if "đánh giá bài viết" in txt.lower() or "ý kiến của bạn" in txt.lower():
                    break
                
                if len(txt) > 20: # Higher threshold for better quality
                    paragraphs.append(txt)
            
            # Additional cleanup of the tail
            final_paragraphs = []
            for p in paragraphs:
                if any(x in p.lower() for x in ["cổng ttđt tp", "thông tin cần biết", "liên kết website"]):
                    continue
                final_paragraphs.append(p)
            
            if final_paragraphs:
                result["content"] = " ".join(final_paragraphs)
            else:
                 result["content"] = clean_text(content_div.get_text())
                 # Final attempt to trim the text if it's too long and contains boilerplate
                 if "Đánh giá bài viết" in result["content"]:
                     result["content"] = result["content"].split("Đánh giá bài viết")[0].strip()

        # KEYWORDS
        meta_kw = soup.find("meta", attrs={"name": "keywords"})
        if meta_kw:
            result["keywords"] = clean_text(meta_kw.get("content"))

        return result

    except Exception as e:
        print(f"Error parsing {url}: {e}")
        return None

# ================= PAGINATION HELPER =================
def get_pagination_params(soup, current_url):
    """
    Finds the 'Next' link and extracts pagination parameters.
    Returns: (base_url_for_pagination, dict_of_params, next_page_number_in_link)
    """
    # Look for "Tiếp theo" or "Next" or class "next"
    next_link = soup.find("a", string=re.compile("Tiếp theo|Next|Sau", re.I))
    if not next_link:
        # Try finding by class
        next_link_li = soup.find("li", class_="next")
        if next_link_li:
            next_link = next_link_li.find("a")
            
    if next_link and next_link.get("href"):
        href = next_link["href"]
        full_url = normalize_url(href)
        
        parsed = urlparse(full_url)
        params = parse_qs(parsed.query)
        
        # Flatten params: parse_qs returns lists, we need single values for requests
        flat_params = {k: v[0] for k, v in params.items()}
        
        return full_url.split("?")[0], flat_params
    
    return None, None

def extract_article_links(soup, seen_urls):
    links = []
    # General strategy: look for links that look like articles in the main content area
    # This might need refinement per category if structure varies wildly
    
    # Try finding asset publisher first
    main_area = soup.find(class_="portlet-asset-publisher") or soup.find(id="main-content") or soup
    
    for a in main_area.find_all("a", href=True):
        href = a["href"]
        # Danang articles often have "/web/dng/-/" or "/chi-tiet"
        if "/web/dng/-/" in href or "/vi/web/dng/-/" in href:
             full_url = normalize_url(href)
             if full_url and full_url not in seen_urls and "danang.gov.vn" in full_url:
                 links.append(full_url)
    
    return list(dict.fromkeys(links))

# ================= MAIN =================
def main():
    seen_urls = set()
    
    # Load existing URLs
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("url"):
                    seen_urls.add(row["url"])
        print(f"Loaded {len(seen_urls)} existing URLs.")
    except FileNotFoundError:
        pass

    # Open file in append mode
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["topic", "title", "summary", "url", "keywords", "public_time", "content"]
        )
        if len(seen_urls) == 0:
            writer.writeheader()

        for topic, start_url in CATEGORIES.items():
            print(f"\n=== Processing Topic: {topic} ===")
            
            # Initial Request to get structure and first page links
            current_url = start_url
            params = {}
            
            # We need to detect the pagination params from the first page
            # to iterate correctly.
            
            try:
                resp = session.get(current_url, timeout=30)
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # Get initial links
                new_links = extract_article_links(soup, seen_urls)
                for link in new_links:
                    seen_urls.add(link)
                    data = parse_article(link, topic)
                    if data and data["title"] and data["content"]:
                        writer.writerow(data)
                        print(f"  [Use First Page] Saved: {data['title'][:50]}...")
                    time.sleep(SLEEP)
                    
                # Setup pagination
                base_page_url, base_params = get_pagination_params(soup, current_url)
                
                if not base_page_url or not base_params:
                    print(f"  Could not find pagination for {topic}. Checking only first page.")
                    continue
                
                # Identify the 'cur' parameter (usually ..._cur)
                cur_key = next((k for k in base_params.keys() if k.endswith("_cur")), None)
                if not cur_key:
                    print(f"  Could not identify 'cur' parameter for {topic}.")
                    continue
                
                print(f"  Pagination detected. Base: {base_page_url}, Cur Param: {cur_key}")
                
                # Iterate pages starting from 2
                # Note: The 'Next' link on page 1 usually points to page 2.
                # We will construct params for page 2, 3, etc.
                
                for page in range(2, MAX_PAGES_PER_TOPIC + 1):
                    print(f"  -> Crawling Page {page} of {topic}...")
                    
                    # Update the current page param
                    base_params[cur_key] = str(page)
                    
                    try:
                        p_resp = session.get(base_page_url, params=base_params, timeout=30)
                        p_soup = BeautifulSoup(p_resp.text, "html.parser")
                        
                        page_links = extract_article_links(p_soup, seen_urls)
                        
                        if not page_links:
                            print("    No new links found. Stopping topic.")
                            break
                            
                        print(f"    Found {len(page_links)} new articles.")
                        
                        count_saved = 0
                        for link in page_links:
                            seen_urls.add(link)
                            data = parse_article(link, topic)
                            if data and data["title"] and data["content"]:
                                writer.writerow(data)
                                count_saved += 1
                                # Minimal logs to keep it clean
                            time.sleep(SLEEP)
                        print(f"    Saved {count_saved} articles.")
                            
                    except Exception as e:
                        print(f"    Error on page {page}: {e}")
                        
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Error initializing topic {topic}: {e}")

if __name__ == "__main__":
    main()
