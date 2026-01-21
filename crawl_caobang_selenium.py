import csv
import time
import os
import html
import re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configuration ---
BASE_URL = "https://caobang.gov.vn"
CATEGORIES = {
    "Chính trị": "https://caobang.gov.vn/chinh-tri",
    "Kinh tế": "https://caobang.gov.vn/kinh-te",
    "Văn hóa - Xã hội": "https://caobang.gov.vn/van-hoaxa-hoi",
    "Quốc phòng - An ninh": "https://caobang.gov.vn/quoc-phong-an-ninh",
    "Tin tức": "https://caobang.gov.vn/chuyen-muc-tin-tuc",
    "Lịch sử - Văn hóa": "https://caobang.gov.vn/lich-su-van-hoa",
    "Di tích": "https://caobang.gov.vn/di-tich",
    "Danh lam thắng cảnh": "https://caobang.gov.vn/danh-lam-thang-canh"
}

OUTPUT_FILE = "caobang_data_final.csv"
MAX_PAGES_PER_CATEGORY = 50 

def clean_text(s):
    if s is None:
        return ""
    s = str(s)
    s = html.unescape(s)
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60) # Set page load timeout
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
        except Exception as e:
            print(f"Error reading existing data: {e}")
    return seen

def extract_article_content(url, topic, retries=1):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    for i in range(retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code != 200:
                print(f"  Failed: {url} (Status: {resp.status_code})")
                continue
                
            soup = BeautifulSoup(resp.text, "html.parser")
            
            title = ""
            title_tag = soup.select_one(".ArticleHeader") or soup.select_one(".title-detail") or soup.find("h1")
            if title_tag:
                title = clean_text(title_tag.get_text())

            public_time = ""
            date_tag = soup.select_one(".PostDate") or soup.select_one(".date-detail") or soup.select_one(".date")
            if date_tag:
                public_time = clean_text(date_tag.get_text())
                
            content = ""
            content_div = soup.select_one(".ArticleContent") or soup.select_one(".newsbody") or soup.select_one(".content-detail")
            if content_div:
                for tag in content_div(["script", "style", "iframe", "form", "div"]):
                     if tag.name != "div" or (tag.name == "div" and not tag.find("p")):
                         tag.decompose()
                paragraphs = [clean_text(p.get_text()) for p in content_div.find_all("p")]
                content = " ".join([p for p in paragraphs if p])

            summary = ""
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                summary = clean_text(meta_desc.get("content"))
            if not summary and content:
                 summary = content.split(".")[0] + "."

            keywords = ""
            meta_kw = soup.find("meta", attrs={"name": "keywords"})
            if meta_kw:
                 keywords = clean_text(meta_kw.get("content"))
                 
            return {
                "topic": topic, "title": title, "summary": summary,
                "url": url, "keywords": keywords, "public_time": public_time, "content": content
            }
        except Exception as e:
            print(f"  Error extracting {url} (attempt {i+1}): {e}")
            if i < retries: time.sleep(2)
            else: return None

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
        print(f"    Scanning Page {page_num}...")
        try:
            all_a = driver.find_elements(By.TAG_NAME, "a")
            page_links = []
            for a in all_a:
                try:
                    href = a.get_attribute("href")
                    if href and re.search(r'-\d+$', href) and "page/" not in href and "javascript" not in href:
                        u = href.split("#")[0].split("?")[0]
                        if u not in global_seen_urls and u not in links_to_crawl:
                            page_links.append(u)
                except: continue
            
            print(f"      Found {len(page_links)} new articles on this page.")
            links_to_crawl.extend(page_links)
            
            if page_num > 1 and not page_links:
                print("      No more new articles found on this page index.")
                if page_num > 5: break # Threshold for early exit

            # Pagination
            if page_num < MAX_PAGES_PER_CATEGORY:
                next_page_num = str(page_num + 1)
                next_btn = driver.find_elements(By.XPATH, f"//a[contains(@class, 'page-link') and normalize-space(text())='{next_page_num}']")
                if not next_btn:
                    next_btn = driver.find_elements(By.XPATH, f"//a[normalize-space(text())='{next_page_num}']")
                
                if next_btn:
                    driver.execute_script("arguments[0].click();", next_btn[0])
                    time.sleep(5)
                else: break
        except Exception as e:
            print(f"    Error during pagination: {e}")
            break
            
    return links_to_crawl

def main():
    global_seen_urls = load_seen_urls(OUTPUT_FILE)
    print(f"Loaded {len(global_seen_urls)} existing URLs.")
    
    for category_name, category_url in CATEGORIES.items():
        print(f"\nProcessing Category: {category_name}")
        
        # 1. Collect links
        driver = init_driver()
        new_urls = []
        try:
            new_urls = collect_category_links(driver, category_url, global_seen_urls)
        finally:
            driver.quit()
        
        if not new_urls:
            print(f"  No new links for {category_name}.")
            continue
        
        print(f"  Collected {len(new_urls)} links for {category_name}. Starting extraction...")
        
        # 2. Extract content (using requests)
        try:
            with open(OUTPUT_FILE, "a", encoding="utf-8-sig", newline="") as f:
                fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if os.path.getsize(OUTPUT_FILE) == 0:
                     writer.writeheader()
                
                for i, url in enumerate(new_urls):
                    details = extract_article_content(url, category_name)
                    if details and details["title"]:
                        writer.writerow(details)
                        print(f"    [{i+1}/{len(new_urls)}] Saved: {details['title'][:50]}...")
                        global_seen_urls.add(url)
                    
                    time.sleep(0.5) # Polite delay
        except Exception as e:
            print(f"  Error during extraction loop: {e}")

    print("\nCrawler finished successfully.")

if __name__ == "__main__":
    main()
