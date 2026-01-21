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
BASE_URL = "https://sonla.gov.vn"
OUTPUT_FILE = "sonla_data_final.csv"
MAX_PAGES_PER_CATEGORY = 100  # Crawl deep
SLEEP_TIME = 2

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
    # Masking automation
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
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
        except Exception as e:
            print(f"Error reading existing data: {e}")
    return seen

def extract_article_content(url, topic, retries=1):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    for i in range(retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=20, verify=False)
            if resp.status_code != 200:
                print(f"    Failed: {url} (Status: {resp.status_code})")
                continue
                
            soup = BeautifulSoup(resp.text, "html.parser")
            
            result = {
                "topic": topic, "title": "", "summary": "", "url": url,
                "keywords": "", "public_time": "", "content": ""
            }

            # 1. Tiêu đề
            title_tag = soup.select_one(".ArticleHeader, .title-news, h1")
            if title_tag:
                 result["title"] = clean_text(title_tag.get_text())
            elif soup.find("meta", attrs={"name": "title"}):
                 result["title"] = clean_text(soup.find("meta", attrs={"name": "title"}).get("content"))
            
            # 2. Thời gian
            time_tag = soup.select_one(".PostDate, .date, .time, .cms-date")
            if time_tag: result["public_time"] = clean_text(time_tag.get_text())

            # 3. TÓM TẮT
            summary_tag = soup.select_one(".ArticleSummary, .summary, .sapo")
            if summary_tag: 
                result["summary"] = clean_text(summary_tag.get_text())
            if not result["summary"]:
                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc: 
                    desc = clean_text(meta_desc.get("content"))
                    if "Cổng thông tin điện tử" not in desc:
                        result["summary"] = desc

            # 4. KEYWORDS
            meta_kw = soup.find("meta", attrs={"name": "keywords"})
            if meta_kw: result["keywords"] = clean_text(meta_kw.get("content"))

            # 5. Nội dung
            content_div = soup.select_one(".ArticleContent, .journal-content-article, #content")
            if content_div:
                # Remove trash
                for trash in content_div.select("script, style, .social-share, .tags, .rating, .tool, .related-news"):
                    trash.decompose()
                result["content"] = clean_text(content_div.get_text())
            
            return result
        except Exception as e:
            print(f"    Error extracting {url} (attempt {i+1}): {e}")
            if i < retries: time.sleep(2)
            else: return None

def process_category(driver, category_name, category_url, global_seen_urls):
    print(f"  Collecting links from: {category_url}")
    try:
        driver.get(category_url)
        time.sleep(5)
    except Exception as e:
        print(f"    Error loading list page: {e}")
        return

    # Open file in append mode once per category (or could be per article, but per page is good balance)
    # Actually, let's keep it simple: open for each batch save or keep open? 
    # To be safe against crashes, we can open/close per page or per article. keeping open per page is good.
    
    for page_num in range(1, MAX_PAGES_PER_CATEGORY + 1):
        print(f"    Scanning Page {page_num}...", end="\r")
        page_links = []
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a")))
            
            elements = driver.find_elements(By.CSS_SELECTOR, ".Title a, h2 a, h3 a, .title-news a, .ArticleInMenu a, .ArticleList a")
            for a in elements:
                try:
                    href = a.get_attribute("href")
                    if href and "sonla.gov.vn" in href and len(href) > 30:
                        if any(x in href for x in ["/admin/", "/login", "/search", "mailto:", "tel:", "format=pdf", "Default.aspx", "pageid=", ".pdf", ".doc"]): continue
                        u = href.split("#")[0]
                        # Collect ALL links to debug pagination
                        page_links.append(u)
                except: continue
            
            # Remove duplicates using set
            page_links = list(dict.fromkeys(page_links))
            
            print(f"      [DEBUG] Page {page_num}: Found {len(page_links)} links (raw).")
            if page_links:
                print(f"      [DEBUG] First link: {page_links[0]}")

            new_count = 0
            links_to_extract = []
            for u in page_links:
                if u not in global_seen_urls:
                    links_to_extract.append(u)
            
            new_count = len(links_to_extract)
            
            if new_count > 0:
                 print(f"    Scanning Page {page_num}: Found {new_count} new articles. Extracting...")
                 
                 # IMMEDIATE EXTRACTION & SAVING
                 with open(OUTPUT_FILE, "a", encoding="utf-8-sig", newline="") as f:
                    fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) == 0:
                         writer.writeheader()
                    
                    for url in links_to_extract:
                        details = extract_article_content(url, category_name)
                        if details and details["title"]:
                            writer.writerow(details)
                            global_seen_urls.add(url)
                 print(f"      > Saved {new_count} articles from page {page_num}.")
            else:
                 print(f"    Scanning Page {page_num}: No new articles found (All {len(page_links)} seen).")

            # Pagination Logic
            # Pagination Logic
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # Strategy: Find active page using data-page, then click next data-page
                # This bypasses text issues and works with the specific JS implementation seen in debug.
                
                next_created = False
                try:
                    # Find active item
                    active_items = driver.find_elements(By.CSS_SELECTOR, "li.page-item.active")
                    current_page_idx = -1
                    
                    if active_items:
                        # Get the one inside the main pagination (ignoring potential others)
                        # We assume the first one usually, or check parent
                        for item in active_items:
                            dp = item.get_attribute("data-page")
                            if dp:
                                current_page_idx = int(dp)
                                break
                    
                    if current_page_idx == -1:
                        # Fallback to finding by URL or other marker if needed, but let's try text scan if data-page missing
                        print("      [DEBUG] Could not find active data-page. Trying text fallback.")
                        pass
                    else:
                        next_page_idx = current_page_idx + 1
                        print(f"      [DEBUG] Current Page: {current_page_idx}. Target Next: {next_page_idx}")
                        
                        # Find LI with data-page = next_page_idx
                        # The JS only shows some pages, so current+1 should be visible unless we are at end
                        next_li = driver.find_elements(By.CSS_SELECTOR, f"li.page-item[data-page='{next_page_idx}'] a")
                        
                        if next_li:
                            print(f"      [DEBUG] Found direct link for page {next_page_idx}. Clicking.")
                            btn = next_li[0]
                            driver.execute_script("arguments[0].click();", btn)
                            next_created = True
                        else:
                            # If direct number not found, maybe look for "Next" button if it exists
                            # But per HTML, maybe there is no explicitly labeled "Next" except ">>" (Last)
                            # Let's check for class 'next' or similar just in case
                            print(f"      [DEBUG] Direct link for {next_page_idx} not found (maybe hidden?). Checking for Next button symbol.")
                except Exception as e_inner:
                     print(f"      [DEBUG] Error determining next page index: {e_inner}")

                if not next_created:
                    # Fallback to the old text-based approach just in case
                    candidates = driver.find_elements(By.CSS_SELECTOR, ".pagination a, .pager a, ul.pagination a, .page-link")
                    for btn in candidates:
                        text = btn.text.strip()
                        # Look for > or Next
                        if text in [">", "Next", "Tiếp", "Trang sau"]:
                             print(f"      [DEBUG] Clicking text-based Next button: '{text}'")
                             driver.execute_script("arguments[0].click();", btn)
                             next_created = True
                             break
                
                if next_created:
                    # Wait for ArticleList to update
                    # Simple sleep is okay, but we can verify url or content change?
                    # Since it is AJAX, URL might NOT change.
                    time.sleep(5) 
                else:
                    print(f"      No next button/link found at page {page_num}. Stopping category.")
                    break
                    
            except Exception as e:
                print(f"      Pagination error/End of list: {e}")
                break

        except Exception as e:
            print(f"    Error processing page {page_num}: {e}")
            break

def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    global_seen_urls = load_seen_urls(OUTPUT_FILE)
    print(f"Loaded {len(global_seen_urls)} existing URLs.")
    
    driver = init_driver()
    
    try:
        # Create file with header if not exists
        if not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0:
            with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
                fieldnames = ["topic", "title", "summary", "url", "keywords", "public_time", "content"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

        for category_name, category_url in CATEGORIES.items():
            print(f"\n--- Processing Category: {category_name} ---")
            process_category(driver, category_name, category_url, global_seen_urls)
            print(f"  Finished {category_name}.")
            
    except KeyboardInterrupt:
        print("\nCrawler stopped by user.")
    except Exception as e:
        print(f"\nCritical Error: {e}")
    finally:
        driver.quit()
        print("\nDriver closed.")

if __name__ == "__main__":
    main()
