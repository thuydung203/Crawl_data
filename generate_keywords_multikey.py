import csv
import os
import time
import requests
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import io

# Force UTF-8 for print statements in Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION ---
API_KEYS = [
    "AIzaSyDAkEH0lSsUoQ0hcmVO2pIocjSwPVsDy8g",
    "AIzaSyD64PqIMZ0dsadCiruULkz2x2lueR5vU80",
    "AIzaSyCTeqndD9hsBr3bOX1AoPz4aUZGcUcbtjE",
    "AIzaSyDGj0bwwfeF-4ql8GDJbN9acBlUueFZWzA",
]

MODEL_NAME = 'gemini-1.5-flash'
INPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop\bacninh_data_final.csv'
OUTPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'bacninh_data_final_AI_Final.csv')

# Processing Config
MAX_WORKERS = 25  # High concurrency
DELAY_PER_KEY_SECONDS = 2.0 # Aggressive speed

PROMPT_TEMPLATE = """
Dựa vào nội dung bài báo sau đây, hãy trích xuất 4-7 từ khóa quan trọng nhất.
Yêu cầu:
- Mỗi từ khóa phải là một cụm từ ngắn gọn (ví dụ: "tuyển sinh đại học", "quy hoạch đô thị"), KHÔNG được là một câu dài.
- Các từ khóa cách nhau bằng dấu phẩy.
- Định dạng kết quả TRẢ VỀ DUY NHẤT danh sách từ khóa, ví dụ: từ khóa 1, từ khóa 2, từ khóa 3
- Không thêm bất kỳ lời dẫn hay giải thích nào khác.

Nội dung bài báo:
{content}
"""

csv.field_size_limit(10 * 1024 * 1024)

# --- KEY MANAGER ---
class KeyManager:
    def __init__(self, keys):
        self.keys = []
        for k in keys:
             self.keys.append({'key': k, 'last_used': 0})
        self.lock = threading.Lock()

    def get_key(self):
        with self.lock:
            now = time.time()
            self.keys.sort(key=lambda x: x['last_used'])
            best_key = self.keys[0]
            
            time_since_last = now - best_key['last_used']
            if time_since_last < DELAY_PER_KEY_SECONDS:
                wait_time = DELAY_PER_KEY_SECONDS - time_since_last
                time.sleep(wait_time)
            
            best_key['last_used'] = time.time()
            return best_key['key']

    def remove_key(self, key_to_remove):
        with self.lock:
            self.keys = [k for k in self.keys if k['key'] != key_to_remove]
            if not self.keys:
                print("  [CRITICAL] All keys have been removed!")

key_manager = KeyManager(API_KEYS)
file_lock = threading.Lock()

# Candidate models to try (in order)
MODEL_CANDIDATES = [
    'gemini-1.5-flash',
    'gemini-1.5-flash-001',
    'gemini-1.5-pro',
    'gemini-1.5-pro-001',
    'gemini-pro',
    'gemini-1.0-pro'
]

def generate_keywords_with_key(content, key):
    if not content or len(content.strip()) < 50:
        return ""
    
    # Truncate content
    content_sample = content[:15000]
    prompt = PROMPT_TEMPLATE.format(content=content_sample)
    data = {"contents": [{"parts": [{"text": prompt}]}]}
    headers = {'Content-Type': 'application/json'}

    last_error = None
    
    for model_name in MODEL_CANDIDATES:
        # Handle models that might already have 'models/' prefix or not
        clean_model_name = model_name.replace('models/', '')
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{clean_model_name}:generateContent?key={key}"
        
        try:
            response = requests.post(url, headers=headers, json=data, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                try:
                    candidates = result.get('candidates', [])
                    if candidates:
                        parts = candidates[0].get('content', {}).get('parts', [])
                        if parts:
                            keywords = parts[0].get('text', '')
                            keywords = keywords.strip().replace('\n', ' ').replace('"', '').rstrip('.')
                            return keywords
                    return ""
                except (KeyError, IndexError) as e:
                    # JSON parse error, maybe try next model? No, 200 means model worked but response weird.
                    # Just return empty here to avoid infinite loops on bad content
                    # print(f"    [Parse Error] {model_name}: {e}")
                    return ""
            
            elif response.status_code == 429: # Quota
                 print(f"    [Quota] Key ...{key[-5:]} (429) on {model_name}.")
                 return None # Signal quota, key manager should sleep
            
            elif response.status_code == 404:
                 # Model not found, TRY NEXT CANDIDATE
                 # print(f"    [404] {model_name} not found for ...{key[-5:]}")
                 continue
                 
            else:
                 print(f"    [Error] Key ...{key[-5:]} {model_name} status {response.status_code}: {response.text[:100]}")
                 return None
                 
        except Exception as e:
            print(f"    [Exception] Key ...{key[-5:]}: {e}")
            return None

    # If we get here, all models failed (likely 404s)
    print(f"    [Failed] Key ...{key[-5:]} failed all model candidates. Removing key.")
    key_manager.remove_key(key)
    return None

def process_row(row, row_index):
    content = row.get('content', '')
    
    # Retry logic
    max_retries = 5 # More retries since we might drop keys
    for attempt in range(max_retries):
        key = key_manager.get_key()
        if not key:
            print("  [Error] No valid keys left!")
            return None

        keywords = generate_keywords_with_key(content, key)
        
        if keywords is not None:
             row['keywords'] = keywords
             if keywords:
                 print(f"  [Row {row_index}] Success: {keywords[:50]}...")
             else:
                 print(f"  [Row {row_index}] Empty/Skipped")
             return row
        
        # If None (error), we loop and get another key (hopefully a good one)
        time.sleep(1)

    print(f"  [Row {row_index}] Failed after retries.")
    return None

def main():
    print(f"Starting High-Speed REST Keyword Generation for Bac Ninh...")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    existing_urls = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('url'):
                    existing_urls.add(row['url'])
    print(f"Found {len(existing_urls)} already processed rows.")

    rows_to_process = []
    fieldnames = []
    with open(INPUT_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for i, row in enumerate(reader):
            if row.get('url') not in existing_urls:
                rows_to_process.append((i+1, row))

    if not rows_to_process:
        print("Job done.")
        return

    print(f"Processing {len(rows_to_process)} rows with {MAX_WORKERS} threads...")

    mode = 'a' if existing_urls else 'w'
    with open(OUTPUT_FILE, mode, encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        if mode == 'w':
            writer.writeheader()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(process_row, row, idx): (idx, row) for idx, row in rows_to_process}

        for future in as_completed(future_map):
            idx, _ = future_map[future]
            try:
                result_row = future.result()
                if result_row:
                    with file_lock:
                        with open(OUTPUT_FILE, 'a', encoding='utf-8-sig', newline='') as f_out:
                            writer = csv.DictWriter(f_out, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                            writer.writerow(result_row)
            except Exception as e:
                print(f"  [Exception] Row {idx}: {e}")

    print("\nProcessing Complete.")

if __name__ == "__main__":
    main()
