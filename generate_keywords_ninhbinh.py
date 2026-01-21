
import csv
import os
import time
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
    "AIzaSyA-CKP73UzTw3IiijZaEDxA7Q7OBMJkDWM"
]

# File Paths
INPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop\ninhbinh_data_final.csv'
OUTPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed\ninhbinh_data_final_AI_Final.csv'

# Processing Limit
LIMIT_ROWS = 50

# Logging
LOG_FILE = "ninhbinh_generation.log"
def log_debug(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{time.strftime('%H:%M:%S')} - {msg}\n")
    print(msg)

if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
    os.makedirs(os.path.dirname(OUTPUT_FILE))

PROMPT_TEMPLATE = """
Dựa vào nội dung bài báo sau đây, hãy trích xuất 4-7 từ khóa quan trọng nhất.
Yêu cầu:
- Mỗi từ khóa phải là một cụm từ ngắn gọn (ví dụ: tuyển sinh đại học, tóm tắt văn bản,...).
- KHÔNG dùng cả câu dài.
- Các từ khóa cách nhau bởi dấu PHẨY.
- Định dạng kết quả trả về: key A, key B, key C
- KHÔNG thêm dấu ngoặc kép vào kết quả (tôi sẽ tự thêm sau).
- KHÔNG thêm bất kỳ lời dẫn nào.

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

# Models to try in order
MODELS_TO_TRY = [
    'gemini-1.5-flash',
    'gemini-1.5-flash-001', 
    'gemini-1.5-pro',
    'gemini-pro'
]

def generate_keywords(content):
    if not content or len(content.strip()) < 50:
        return ""
    
    content_sample = content[:3000]
    
    # Retry loop with key rotation and model fallback
    max_retries = 5 # Increased retries
    
    for attempt in range(max_retries):
        key = get_next_key()
        genai.configure(api_key=key)
        
        for model_name in MODELS_TO_TRY:
            try:
                # log_debug(f"Trying Key: ...{key[-5:]} | Model: {model_name}")
                model = genai.GenerativeModel(model_name)
                time.sleep(2) # Mandatory sleep between requests
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
                    return f'"{keywords}"'
            
            except Exception as e:
                err_msg = str(e).lower()
                if "404" in err_msg or "not found" in err_msg:
                    # Model not supported by this key, try next model
                    continue
                elif "quota" in err_msg or "429" in err_msg:
                    log_debug(f"  [Quota] {model_name} on ...{key[-5:]}: 429. Switching...")
                    # If quota, break model loop to switch key (outer loop)
                    # changing key might help if other key has quota
                    break 
                else:
                    log_debug(f"  [Error] {model_name} on ...{key[-5:]}: {e}")
                    # Other error, maybe try next model?
                    continue
        
        # If we broke out of model loop due to quota (or finished models), we go to next attempt (next key)
        # Add a small sleep to avoid hammering
        time.sleep(2)
    
    log_debug("Failed to generate after all retries.")
    return ""

def process_file():
    print(f"\n--- Processing Ninh Binh Data (Limit: {LIMIT_ROWS}) ---")
    
    rows = []
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8-sig') as f_in:
            reader = csv.DictReader(f_in)
            for row in reader:
                rows.append(row)
    except FileNotFoundError:
        print(f"File not found: {INPUT_FILE}")
        return

    total_rows = len(rows)
    print(f"Total rows in input: {total_rows}")

    # Fieldnames
    FIELDNAMES = ['topic', 'title', 'summary', 'url', 'keywords', 'public_time', 'content']

    processed_count = 0
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8-sig', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for i, row in enumerate(rows):
            if processed_count >= LIMIT_ROWS:
                print(f"Reached limit of {LIMIT_ROWS} rows. Stopping.")
                break

            content = row.get('content', '')
            keywords = ""
            
            if content:
                print(f"[{i+1}/{total_rows}] Generating for: {row.get('url', '')}")
                keywords = generate_keywords(content)
                print(f"  -> {keywords}")
            
            row['keywords'] = keywords
            
            output_row = {k: row.get(k, '') for k in FIELDNAMES}
            writer.writerow(output_row)
            f_out.flush()
            
            processed_count += 1
            
            # Rate limit delay
            time.sleep(4.0) 

    print("Done.")

if __name__ == "__main__":
    process_file()
