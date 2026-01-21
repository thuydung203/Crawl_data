
import csv
import time
import os
import html
import re
import requests
import urllib3
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
BASE_URL = "https://www.dienbien.gov.vn"
CATEGORIES = {
    "Hoạt động lãnh đạo UBND tỉnh": "https://www.dienbien.gov.vn/portal/Pages/Hoat-dong-lanh-dao-UBND-tinh.aspx",
    "Tin chỉ đạo điều hành": "https://www.dienbien.gov.vn/portal/Pages/Tin-chi-dao-dieu-hanh.aspx",
    "Tin tức - Sự kiện": "https://www.dienbien.gov.vn/portal/Pages/Tin-tuc-su-kien.aspx",
    "Thông tin trong tỉnh": "https://www.dienbien.gov.vn/portal/Pages/Thong-tin-trong-tinh.aspx",
    "Thông tin trong nước và quốc tế": "https://www.dienbien.gov.vn/portal/Pages/Thong-tin-trong-nuoc-va-quoc-te.aspx",
    "Kinh tế - Xã hội": "https://www.dienbien.gov.vn/portal/Pages/Chuyen-muc-Kinh-te-Xa-hoi.aspx",
    "Quốc phòng - An ninh": "https://www.dienbien.gov.vn/portal/Pages/Quoc-phong-an-ninh.aspx",
    "Văn hóa - Du lịch": "https://www.dienbien.gov.vn/portal/Pages/Van-hoa-Du-lich.aspx",
    "Giáo dục - Y tế": "https://www.dienbien.gov.vn/portal/Pages/Giao-duc-Y-te.aspx",
    "Khoa học - Công nghệ": "https://www.dienbien.gov.vn/portal/Pages/Khoa-hoc-Cong-nghe.aspx",
    "Môi trường - Đô thị": "https://www.dienbien.gov.vn/portal/Pages/Moi-truong-Do-thi.aspx",
    "Cải cách hành chính": "https://www.dienbien.gov.vn/portal/Pages/Chuyen-muc-Cai-cach-hanh-chinh.aspx",
    "Hoạt động của các ngành, địa phương": "https://www.dienbien.gov.vn/portal/Pages/Hoat-dong-cua-cac-nganh-dia-phuong.aspx",
}

OUTPUT_FILE = "dienbien_data_final.csv"
MAX_PAGES_PER_CATEGORY = 500

def clean_text(s):
    if s is None: return ""
    s = str(s).strip()
    s = html.unescape(s)
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def init_driver():
    chrome_options = Options()
    # chrome_options.add_argument("--headless") # Headless
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver

def load_seen_urls(filepath):
    seen = set()
    if os.path.exists(filepath):
        try:
            with open(filepath, mode="r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if "url" in row:
                        seen.add(row["url"])
        except Exception:
            pass
    return seen

def parse_html_content(soup, url, topic):
    # TITLE
    title = ""
    title_tag = (soup.select_one(".tandan-title-view") or 
                 soup.select_one(".title-news") or 
                 soup.find("h1"))
                 
    if title_tag:
        title = clean_text(title_tag.get_text())
    
    # Validation: If title is generic site title, ignore it
    if "CỔNG THÔNG TIN ĐIỆN TỬ" in title.upper():
        # Try to find another h1 or title class
        title = ""

    # DATE
    public_time = ""
    date_tag = (soup.select_one(".tandan-date-view") or 
                soup.select_one(".date-news") or 
                soup.select_one(".timer"))
    if date_tag:
        public_time = clean_text(date_tag.get_text())
        
    # CONTENT
    content = ""
    content_div = (soup.select_one(".tandan-content-view") or 
                   soup.select_one(".news-content") or 
                   soup.select_one("#div_content") or
                   soup.select_one(".content-detail"))
                   
    if content_div:
        # cleanup
        for tag in content_div(["script", "style", "iframe", "form", "div"]): 
             if tag.name == "div" and tag.get("class") and "external" in tag.get("class"): pass
             elif tag.name != "div" or not tag.get_text(strip=True): tag.decompose()
        
        paragraphs = [clean_text(p.get_text()) for p in content_div.find_all("p")]
        content = " ".join([p for p in paragraphs if p])
        if not content: content = clean_text(content_div.get_text())

    # SUMMARY
    summary = ""
    summary_div = soup.select_one(".summary-news") or soup.select_one(".news-sapo")
    if summary_div:
        summary = clean_text(summary_div.get_text())
    if not summary:
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc: summary = clean_text(meta_desc.get("content"))
    if not summary and content:
         summary = content[:200] + "..."

    # KEYWORDS
    keywords = ""
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw:
         keywords = clean_text(meta_kw.get("content"))
         
    return {
        "topic": topic, "title": title, "summary": summary,
        "url": url, "keywords": keywords, "public_time": public_time, "content": content
    }

def extract_article_hybrid(driver, url, topic, retries=1):
    # 1. Try Requests (Fast)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    try:
        resp = requests.get(url, headers=headers, timeout=15, verify=False)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            data = parse_html_content(soup, url, topic)
            if data and data["title"] and data["content"]:
                return data
            # Else fall through to Selenium
    except Exception as e:
        print(f"  Requests failed for {url}: {e}")

    # 2. Try Selenium (Slow but Robust)
    print(f"  > Fallback to Selenium for: {url}")
    try:
        driver.get(url)
        # Wait for title
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".tandan-title-view, .title-news, h1"))
            )
        except: pass
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        data = parse_html_content(soup, url, topic)
        
        # Last ditch: Title from <title> tag if H1 missing
        if data and not data["title"]:
             page_title = driver.title
             if page_title:
                 # Cleanup "- Cổng thông tin..."
                 if " - CỔNG THÔNG TIN" in page_title:
                     data["title"] = page_title.split(" - CỔNG THÔNG TIN")[0].strip()
                 else:
                     data["title"] = page_title
        
        return data
    except Exception as e:
        print(f"  Selenium extraction failed: {e}")
        return None

def collect_category_links(driver, category_url, global_seen_urls):
    links_to_crawl = []
    print(f"  Collecting links from: {category_url}")
    try:
        driver.get(category_url)
        time.sleep(5)
    except Exception as e:
        print(f"    Error loading list page: {e}")
        return links_to_crawl

    for page_num in range(1, MAX_PAGES_PER_CATEGORY + 1):
        print(f"    Scanning Page {page_num}/{MAX_PAGES_PER_CATEGORY}...", end="\r")
        
        try:
            # Scraping Links
            elements = driver.find_elements(By.CSS_SELECTOR, ".channel-news-title a, .tandan-p-article-news-title a, .title-news a")
            
            page_current_urls = []
            for el in elements:
                try:
                    href = el.get_attribute("href")
                    if href and "/portal/Pages/" in href and ".aspx" in href and "javascript" not in href:
                        clean_href = href.split("#")[0].split("?")[0]
                        if not any(x in clean_href.lower() for x in ["default.aspx", "login.aspx"]):
                            if clean_href not in global_seen_urls and clean_href not in links_to_crawl:
                                page_current_urls.append(clean_href)
                except: continue
            
            if page_current_urls:
                links_to_crawl.extend(page_current_urls)
            
            # Pagination
            next_page = page_num + 1
            next_btn = None
            try:
                next_btn = driver.find_element(By.XPATH, f"//a[normalize-space(text())='{next_page}']")
            except:
                pass
            
            if not next_btn:
                try:
                    next_btn = driver.find_element(By.XPATH, "//a[contains(text(), 'Tiếp') or contains(text(), 'Next') or text()='>']")
                except:
                    pass
            
            if next_btn:
                try:
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(5)
                except Exception as e:
                    print(f"\n    Failed to click Next: {e}")
                    break
            else:
                print(f"\n    No 'Next' button or Page {next_page} found. Stopping.")
                break
                
        except Exception as e:
            print(f"\n    Error processing page {page_num}: {e}")
            break
            
    print(f"\n    Category collection finished. Total links found: {len(links_to_crawl)}")
    return links_to_crawl

def main():
    global_seen_urls = load_seen_urls(OUTPUT_FILE)
    print(f"Loaded {len(global_seen_urls)} existing URLs.")
    
    try:
        driver = init_driver()
        print("Driver initialized successfully.")
    except Exception as e:
        print(f"CRITICAL ERROR: Selenium driver init failed: {e}")
        return

    try:
        for category_name, category_url in CATEGORIES.items():
            print(f"\n[Category]: {category_name}")
            
            new_urls = collect_category_links(driver, category_url, global_seen_urls)
            
            if not new_urls: move_on = True # Just to show logic flow
            
            print(f"  Extracting details for {len(new_urls)} articles...")
            
            mode = "a"
            write_header = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0
            
            with open(OUTPUT_FILE, mode, encoding="utf-8-sig", newline="") as f:
                fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                    
                for i, url in enumerate(new_urls):
                    if url in global_seen_urls: continue
                    
                    # Use Hybrid Extraction
                    data = extract_article_hybrid(driver, url, category_name)
                    
                    if data:
                        writer.writerow(data)
                        f.flush()
                        print(f"    [{i+1}/{len(new_urls)}] Saved: {data['title'][:50]}...")
                        global_seen_urls.add(url)
                    else:
                        print(f"    [{i+1}/{len(new_urls)}] Skipped (No Data): {url}")
            
    finally:
        driver.quit()
        print("\n--- Crawler Completed ---")

if __name__ == "__main__":
    main()
