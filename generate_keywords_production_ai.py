
import csv
import os
import time
import glob
import google.generativeai as genai
import sys
import io

# Force UTF-8 for print statements in Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION ---
API_KEYS = [
    "AIzaSyCv_rfAb_pFtcsEvlOJJ7FQQ66whPOgLJE",
    "AIzaSyByEM_ujzyICJOcegPq54era0N8ftq-OrY"
]

# Models to try (fallback strategy)
MODELS_TO_TRY = [
    'gemini-2.0-flash-exp'
]

# File Paths
INPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop'
OUTPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed'

# Logging
LOG_FILE = "generation_debug.log"
def log_debug(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{time.strftime('%H:%M:%S')} - {msg}\n")
    print(msg)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


PROMPT_TEMPLATE = """
Dựa vào nội dung bài báo sau đây, hãy trích xuất 4-7 từ khóa quan trọng nhất.
Yêu cầu:
- Mỗi từ khóa phải là một cụm từ ngắn gọn.
- Các từ khóa cách nhau bằng dấu phẩy.
- Định dạng kết quả TRẢ VỀ DUY NHẤT danh sách từ khóa.
- Không thêm bất kỳ lời dẫn hay giải thích nào khác.

Nội dung bài báo:
{content}
"""

csv.field_size_limit(10 * 1024 * 1024)

current_key_index = 0

def get_next_key():
    global current_key_index
    key = API_KEYS[current_key_index]
    current_key_index = (current_key_index + 1) % len(API_KEYS)
    return key

def generate_keywords(content):
    if not content or len(content.strip()) < 50:
        return ""
    
    content_sample = content[:15000]
    
    # Infinite retry loop for Quota
    while True:
        # Rotate key for every attempt to spread load
        key = get_next_key()
        genai.configure(api_key=key)
        
        try:
            # Always force gemini-2.0-flash-exp for this key
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            response = model.generate_content(
                PROMPT_TEMPLATE.format(content=content_sample),
                generation_config=genai.types.GenerationConfig(
                    temperature=0.3,
                ),
                request_options={'timeout': 60}
            )
            
            if response and response.text:
                keywords = response.text.strip().replace('\n', ' ').replace('"', '').strip()
                keywords = keywords.rstrip('.')
                return keywords
            else:
                 log_debug("Empty text response")
                 return ""

        except Exception as e:
            err_msg = str(e).lower()
            if "exhausted" in err_msg or "429" in err_msg:
                log_debug(f"  [Quota] Key ...{key[-5:]} Hit. Switching/Waiting...")
                # Short sleep then retry (next key will be picked up)
                time.sleep(5) 
                continue 
            elif "404" in err_msg:
                 log_debug(f"  [404] Model not found on ...{key[-5:]}? {e}")
                 # If model missing on one key, maybe present on other? 
                 # But usually consistent. Retry anyway with next key?
                 time.sleep(2)
                 continue
            elif "safety" in err_msg:
                 return '"Nội dung nhạy cảm"'
            else:
                log_debug(f"  [Error] {e}. Waiting 5s...")
                time.sleep(5)
                continue
    return ""

def process_all_files():
    # Find all CSV files in Tong_hop
    files = glob.glob(os.path.join(INPUT_DIR, "*_data_final.csv"))
    
    # Prioritize Bac Ninh as requested
    files.sort(key=lambda x: "bacninh" not in os.path.basename(x).lower())
    
    print(f"Found {len(files)} files to process.")

    for file_path in files:
        file_name = os.path.basename(file_path)
        output_path = os.path.join(OUTPUT_DIR, file_name.replace(".csv", "_AI_Final.csv"))
        
        print(f"\n--- Processing: {file_name} ---")
        
        # Load existing progress if any
        existing_urls = set()
        if os.path.exists(output_path):
            try:
                with open(output_path, 'r', encoding='utf-8-sig') as f_exist:
                    reader = csv.DictReader(f_exist)
                    for row in reader:
                        if row.get('url') and row.get('keywords') and len(row['keywords'].strip()) > 5:
                            existing_urls.add(row['url'])
                print(f"Resuming from existing file. {len(existing_urls)} rows already done.")
            except Exception as e:
                print(f"Error reading existing file: {e}")

        # Read input file
        rows = []
        with open(file_path, 'r', encoding='utf-8-sig') as f_in:
            reader = csv.DictReader(f_in)
            for row in reader:
                rows.append(row)

        total_rows = len(rows)
        processed_in_this_run = 0

        # Define STRICT column order as requested
        # topic,title,summary,url,keywords,public_time,content
        FIELDNAMES = ['topic', 'title', 'summary', 'url', 'keywords', 'public_time', 'content']

        # Open output file in append/write mode
        mode = 'a' if existing_urls and os.path.exists(output_path) else 'w'
        write_header = (mode == 'w')
        
        with open(output_path, mode, encoding='utf-8-sig', newline='') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
            if write_header:
                writer.writeheader()

            for i, row in enumerate(rows):
                url = row.get('url')
                if url in existing_urls:
                    continue
                
                content = row.get('content', '')
                if not content:
                    row['keywords'] = ""
                else:
                    keywords = generate_keywords(content)
                    row['keywords'] = keywords
                    processed_in_this_run += 1
                    print(f"  [{i+1}/{total_rows}] {file_name} -> {keywords[:50]}...")
                
                # Ensure row only has keys in FIELDNAMES
                output_row = {k: row.get(k, '') for k in FIELDNAMES}
                # Fix public_time if needed or just pass through
                if 'keywords' in row:
                    output_row['keywords'] = row['keywords']
                
                writer.writerow(output_row)
                f_out.flush() 
                
                # Mandated delay for 2.0 Flash Exp (very strict)
                print(f"  [Sleep] Waiting 15s for rate limit...")
                time.sleep(15.0)

        print(f"Finished {file_name}. Generated {processed_in_this_run} new entries.")

if __name__ == "__main__":
    process_all_files()
