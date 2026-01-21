import requests
from bs4 import BeautifulSoup
import csv
import time
import re
from urllib.parse import urlparse, parse_qs, urljoin
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# Configuration
BASE_URL = "https://tuyengiaodanvan.vn/"
OUTPUT_FILE = "tuyengiao_data.csv"
MAX_PAGES_PER_TOPIC = 50 

# Topic Mapping - Comprehensive list from research
TOPICS = {
    # CHỈ ĐẠO, ĐỊNH HƯỚNG
    "Chỉ đạo - Lý luận chính trị": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d37460777e499c87d270",
    "Chỉ đạo - Báo chí - Xuất bản": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d37d60777e499c87d271",
    "Chỉ đạo - Dân chủ và Dân vận": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d38460777e499c87d272",
    "Chỉ đạo - Mặt trận và Đoàn thể": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d38c60777e499c87d273",
    "Chỉ đạo - Thông tin - Tuyên truyền": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d39a60777e499c87d274",
    "Chỉ đạo - Văn hóa - Văn nghệ": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d3a260777e499c87d275",
    "Chỉ đạo - Dân tộc - Tôn giáo": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d3a960777e499c87d276",
    "Chỉ đạo - Khoa giáo": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d3b160777e499c87d277",
    "Chỉ đạo - Bảo vệ nền tảng tư tưởng": "https://tuyengiaodanvan.vn/blogs/category/type/chi-dao-dinh-huong?&categoryId=67f4d3bd60777e499c87d278",
    
    # TIN TỨC, SỰ KIỆN
    "Tin tức - Dư luận xã hội": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4cbbd48ad87279a8a7692",
    "Tin tức - Lý luận chính trị": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4cbd048ad87279a8a7693",
    "Tin tức - Báo chí - Xuất bản": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4cbe448ad87279a8a7694",
    "Tin tức - Dân chủ và Dân vận": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4cbf848ad87279a8a7695",
    "Tin tức - Mặt trận và Đoàn thể": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4cc0348ad87279a8a7696",
    "Tin tức - Thông tin - Tuyên truyền": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4d26a60777e499c87d25f",
    "Tin tức - Văn hóa - Văn nghệ": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4d27660777e499c87d260",
    "Tin tức - Dân tộc - Tôn giáo": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4d28360777e499c87d261",
    "Tin tức - Khoa giáo": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4d28d60777e499c87d262",
    "Tin tức - Bảo vệ nền tảng tư tưởng": "https://tuyengiaodanvan.vn/blogs/category/type/tin-tuc-su-kien?&categoryId=67f4d2a160777e499c87d263",

    # NGHIÊN CỨU - TRAO ĐỔI
    "Nghiên cứu - Dư luận xã hội": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4ca9a48ad87279a8a7691",
    "Nghiên cứu - Bảo vệ nền tảng": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d2d460777e499c87d264",
    "Nghiên cứu - Học tập theo Bác": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d2e560777e499c87d265",
    "Nghiên cứu - Lịch sử Đảng": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d2f760777e499c87d266",
    "Nghiên cứu - Báo chí - Xuất bản": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d30160777e499c87d267",
    "Nghiên cứu - Lý luận chính trị": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d30b60777e499c87d268",
    "Nghiên cứu - Văn hóa - Văn nghệ": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d32860777e499c87d26a",
    "Nghiên cứu - Khoa giáo": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d33260777e499c87d26b",
    "Nghiên cứu - Thông tin - Tuyên truyền": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d33a60777e499c87d26c",
    "Nghiên cứu - Dân chủ và Dân vận": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d34160777e499c87d26d",
    "Nghiên cứu - Mặt trận và Đoàn thể": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d34760777e499c87d26e",
    "Nghiên cứu - Điển hình dân vận": "https://tuyengiaodanvan.vn/blogs/category/type/nghien-cuu-trao-doi?&categoryId=67f4d35760777e499c87d26f",
    
    # KHÁC
    "Văn bản": "https://tuyengiaodanvan.vn/van-ban"
}

def init_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.page_load_strategy = 'eager' # Faster
    return webdriver.Chrome(options=options)

def resolve_topic_urls(driver):
    """Visit homepage and map topic names to real URLs with categoryId"""
    print("Resolving real topic URLs from homepage...")
    try:
        driver.get(BASE_URL)
        time.sleep(5)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        links = soup.select("a")
        
        # Normalize helper
        def norm(s): return re.sub(r'\s+', ' ', s).strip().lower()

        # Build map of text -> href from page
        page_links = {}
        for a in links:
            txt = norm(a.get_text())
            href = a.get('href')
            if href and len(txt) > 3:
                if not href.startswith('http'):
                    href = urljoin(BASE_URL, href)
                page_links[txt] = href
        
        # Match configured topics
        for name, dummy_url in TOPICS.items():
            found = False
            # 1. Exact match (normalized)
            n_name = norm(name)
            if n_name in page_links:
                TOPICS[name] = page_links[n_name]
                print(f"  [OK] '{name}' -> {TOPICS[name]}")
                found = True
            
            # 2. Partial match
            if not found:
                for p_txt, p_href in page_links.items():
                    if n_name in p_txt or n_name.replace(',', ' -') in p_txt or n_name.replace(',', '-') in p_txt:
                        TOPICS[name] = p_href
                        print(f"  [OK] '{name}' -> {TOPICS[name]} (Partial: '{p_txt}')")
                        found = True
                        break
            
            # 3. Special Keyword match for difficult ones
            if not found:
                 keywords = n_name.split()
                 if len(keywords) > 2:
                     for p_txt, p_href in page_links.items():
                         if keywords[0] in p_txt and keywords[-1] in p_txt:
                              TOPICS[name] = p_href
                              print(f"  [OK] '{name}' -> {TOPICS[name]} (Keyword match: '{p_txt}')")
                              found = True
                              break

            if not found:
                print(f"  [FAIL] Could not resolve '{name}'. Keeping original (likely broken).")

    except Exception as e:
        print(f"Error resolving links: {e}")

def clean_text(text):
    if not text:
        return ""
    # Remove multiple spaces and newlines
    return re.sub(r'\s+', ' ', text).strip()

def parse_date(date_str):
    if not date_str:
        return ""
    date_str = clean_text(date_str)
    # Handle forms like "Thứ Hai, 19/01/2026..."
    try:
        match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', date_str)
        if match:
            day, month, year = match.groups()
            return f"{int(day):02d}/{int(month):02d}/{year}"
    except:
        pass
    return date_str

def clean_content(raw_html, title=None):
    if not raw_html:
        return ""
    
    soup = BeautifulSoup(raw_html, "html.parser")
    
    # Aggressive removal of non-content elements
    for tag in soup(["script", "style", "iframe", "noscript", "input", "button", "form", "svg", "nav", "footer", "header"]):
        tag.decompose()
        
    for tag in soup(["img", "figure", "figcaption", "table", "video", "audio"]):
        tag.decompose()
    
    # Remove specific classes seen in garbage output
    garbage_classes = ['breadcrumb', 'title', 'date', 'time', 'share', 'related', 'tags', 'meta', 'author', 'print']
    for cls in garbage_classes:
        for tag in soup.find_all(class_=lambda x: x and cls in x.lower()):
            tag.decompose()
            
    # Remove elements containing specific garbage text (case insensitive)
    garbage_text = ['Chia sẻ:', 'Tin liên quan', 'Mới nhất', 'Dữ liệu đang được cập nhật', 'In bài viết']
    for txt in garbage_text:
        for tag in soup.find_all(text=re.compile(txt, re.I)):
            parent = tag.parent
            if parent: parent.decompose()

    # Remove style attributes
    for tag in soup.find_all(True):
        if tag.has_attr('style'):
            del tag['style']
    
    # Extract text
    text = soup.get_text(separator="\n")
    
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    
    final_lines = []
    for line in lines:
        # Filter breadcrumbs (usually uppercase or with splitted chars)
        if line.isupper() and len(line) < 50:
            continue
        # Filter title repeat
        if title and title.lower() in line.lower():
            continue
        # Filter date/time lines
        if re.search(r'^\w+,\s*\d{1,2}/\d{1,2}/\d{4}', line):
            continue
        # Filter tiny lines (often artifacts)
        if len(line) < 5 and not re.match(r'^\d+\.', line): # Allow listed items "1."
            continue
        # Filter image captions
        if len(line) < 150 and re.search(r'^(Ảnh|Nguồn|Theo):', line, re.I):
            continue
            
        final_lines.append(line)
        
    # Join with space to avoid CSV line breaking (User preference for "Super Clean")
    # OR join with ' ' if they want single line
    return " ".join(final_lines)

def process_detail(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "h1"))
        )
        time.sleep(1)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        title_tag = soup.find("h1")
        title = clean_text(title_tag.get_text()) if title_tag else ""
        
        # Date extraction - Look for regex pattern in body first
        public_time = ""
        date_pattern = r'(thứ \w+,\s*)?(\d{2}/\d{2}/\d{4})'
        body_text = soup.get_text()
        date_match = re.search(date_pattern, body_text, re.I)
        if date_match:
            public_time = date_match.group(2)

        # Summary - Often in <i> tag or first bold paragraph
        summary = ""
        summary_candidates = soup.find_all(["i", "p"])
        for cand in summary_candidates:
            txt = clean_text(cand.get_text())
            if not txt: continue
            # Skip if it's just a date
            if re.search(r'^\w+,\s*\d{2}/\d{2}/\d{4}', txt, re.I):
                continue
            if len(txt) > 30:
                summary = txt
                break
        
        # Content - Filter out common boilerplate
        content = ""
        all_ps = soup.find_all("p")
        content_lines = []
        boilerplate = [
            "6C Hoàng Diệu, Ba Đình, Hà Nội",
            "Bản quyền thuộc về Ban Tuyên giáo",
            "Ghi rõ nguồn",
            "phát hành lại thông tin từ trang web này",
            "Xem toàn văn Kế hoạch",
            "Xem nội dung Hướng dẫn"
        ]
        
        for p in all_ps:
            txt = clean_text(p.get_text())
            if not txt: continue
            
            # Skip summary repeat
            if summary and summary in txt:
                continue
            
            # Skip date lines
            if re.search(r'^\w+,\s*\d{2}/\d{2}/\d{4}', txt, re.I):
                continue
                
            # Skip boilerplate
            is_boilerplate = False
            for bp in boilerplate:
                if bp.lower() in txt.lower():
                    is_boilerplate = True
                    break
            if is_boilerplate:
                continue
                
            if len(txt) > 20:
                content_lines.append(txt)
        
        content = " ".join(content_lines)
        
        # Meta Keywords
        keywords = ""
        meta_kw = soup.find("meta", attrs={"name": "keywords"})
        if meta_kw:
            keywords = clean_text(meta_kw.get("content", ""))
        
        return title, summary, public_time, content, keywords
        
    except Exception as e:
        print(f"Error crawling detail {url}: {e}")
        return None, None, None, None, None

def build_page_url(base_url, page_num):
    # React site uses page and size parameters
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page_num}&size=10"

def crawl():
    driver = init_driver()
    
    # 1. Resolve URLs (Disabled - using hardcoded real URLs)
    # resolve_topic_urls(driver)
    
    # 2. Crawl
    with open(OUTPUT_FILE, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["topic", "title", "summary", "url", "keywords", "public_time", "content"])
        
        for config_name, topic_url in TOPICS.items():
            if ".aspx" in topic_url and "category" not in topic_url:
                 print(f"Skipping unresolved topic: {config_name}")
                 continue
                 
            print(f"Processing Topic: {config_name}")
            seen_urls = set()
            
            for page in range(1, MAX_PAGES_PER_TOPIC + 1):
                page_url = build_page_url(topic_url, page)
                print(f"  Page {page}: {page_url}")
                
                try:
                    driver.get(page_url)
                    time.sleep(3)
                    
                    # Try to get topic name from page if possible
                    # but fallback to config_name
                    
                    try:
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/blogs/']"))
                        )
                    except:
                        print("    No articles found or timeout.")
                        if page > 1: break
                        continue # Try next page or topic
                        
                    # More specific link selector for React site
                    all_links = driver.find_elements(By.CSS_SELECTOR, "a[class*='link_link'][href*='/blogs/']")
                    if not all_links:
                        all_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/blogs/']")

                    page_urls = []
                    for a in all_links:
                        href = a.get_attribute('href')
                        if not href: continue
                        # Exclude category list links
                        if '/blogs/' in href and '/category/' not in href:
                             if href not in seen_urls:
                                 seen_urls.add(href)
                                 page_urls.append(href)
                    
                    print(f"    Found {len(page_urls)} unique articles.")
                    
                    if len(page_urls) == 0 and page > 1:
                        print("    End of topic.")
                        break
                    
                    if len(page_urls) == 0:
                        continue # Retry next page? OR break? let's continue for page 1

                    for i, url in enumerate(page_urls):
                        print(f"      Fetching {i+1}/{len(page_urls)}: {url[:60]}...")
                        title, summary, time_str, content, keywords = process_detail(driver, url)
                        if title and content:
                            writer.writerow([config_name, title, summary, url, keywords, time_str, content])
                            f.flush()
                        
                        # Return to list page
                        driver.get(page_url)
                        time.sleep(1) # Quick wait
                        
                except Exception as e:
                    print(f"  Error on page {page}: {e}")
                    break     
    driver.quit()
    print("Crawling complete.")

if __name__ == "__main__":
    crawl()
