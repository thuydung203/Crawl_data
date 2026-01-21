import csv
import os
import time
import google.generativeai as genai
import sys
import io
import glob

# Force UTF-8 for print statements in Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION (ALL 11 KEYS) ---
API_KEYS = [
    "AIzaSyA5GoLoqw9zqkFw5hKSNq5W_NWqNPSoYTM",
    "AIzaSyAt_WQ1YJ0pejdp0DDLjbGOEP3oPdD_Q-8",
    "AIzaSyAA04SvI48CLHMc9ftahH_Hpx3XL90iGmM",
    "AIzaSyBWrAUNzCaD9X1125nQHTkvS-EA3-2gKAY",
    "AIzaSyBqFUEeFspK8XYQ30HO7x0cG61nVbQfcFg",
    "AIzaSyBpLrplS58Lexg0vlps1wlh-i-SRfZmslQ",
    "AIzaSyAPtxXcVefvF5LKGtmi-3wPDPxwj-Wf8qk",
    "AIzaSyDmvIE9HFHv0YP-BRAbeWLqJQzM00dPZOQ",
    "AIzaSyASBl3JGSv_51FyUxcQ3tBkTb9x3SN8cKs",
    "AIzaSyALMlljxJHBszgwuOAuQx1pHjQ_-aqE5dw",
    "AIzaSyDxAuZsHAwf71gDEkOfpo9MPTOeU8ZQoGY",
]

TARGET_MODEL = 'models/gemini-flash-latest'

INPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop'
OUTPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed'

# ULTRA-STABLE DELAY
DELAY_BETWEEN_CALLS = 60.0 

PROMPT_TEMPLATE = """
Dựa vào nội dung bài báo sau đây, hãy trích xuất 4-7 từ khóa quan trọng nhất.
Yêu cầu:
- Mỗi từ khóa là một cụm từ ngắn gọn hoặc danh từ riêng.
- Các từ khóa cách nhau bởi dấu phẩy.
- Đừng trả về bất cứ điều gì khác ngoài các từ khóa.
- Viết hoa các danh từ riêng (Ví dụ: Ban Bí thư, Bắc Ninh, Quy định 396-QĐ/TW).
- Ví dụ định dạng: Ban Bí thư, trường chính trị chuẩn, Quy định 396-QĐ/TW, đào tạo cán bộ, nâng cao chất lượng

Nội dung:
{content}
"""

FIELDNAMES = ['topic', 'title', 'summary', 'url', 'keywords', 'public_time', 'content']
csv.field_size_limit(10 * 1024 * 1024)

def clean_kw(text):
    if not text: return ""
    text = text.replace('**', '').replace('"', '').replace('\n', ' ').strip()
    if text.endswith('.'): text = text[:-1]
    return text

def generate_for_row(content, key_idx):
    key = API_KEYS[key_idx]
    genai.configure(api_key=key)
    try:
        model = genai.GenerativeModel(TARGET_MODEL)
        response = model.generate_content(
            PROMPT_TEMPLATE.format(content=content[:15000]),
            request_options={'timeout': 60}
        )
        if response and response.text:
            return clean_kw(response.text)
    except Exception as e:
        err = str(e).lower()
        if "429" in err or "quota" in err: return "QUOTA"
        if "safety" in err: return "SAFETY"
        if "400" in err: return "INVALID"
        print(f"  [ERROR] Key ...{key[-5:]}: {e}")
    return None

def write_output_file(output_path, successful_rows):
    """Rewrites the output file with only the successful data."""
    with open(output_path, 'w', encoding='utf-8-sig', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in successful_rows:
            writer.writerow({k: row.get(k, '') for k in FIELDNAMES})

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    
    all_files = glob.glob(os.path.join(INPUT_DIR, "*_data_final.csv"))
    all_files.sort(key=lambda x: "bacninh" not in os.path.basename(x).lower())
    
    key_idx = 0
    
    for file_path in all_files:
        file_name = os.path.basename(file_path)
        output_path = os.path.join(OUTPUT_DIR, file_name.replace(".csv", "_AI_Final.csv"))
        
        print(f"\n--- CLEAN START: {file_name} ---")
        
        # 1. Load successful rows
        successful_rows = []
        successful_urls = set()
        if os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    kw = row.get('keywords') or ""
                    # Keep if valid and not an error marker
                    if row.get('url') and kw and len(kw) > 3 and "Lỗi" not in kw:
                        successful_rows.append(row)
                        successful_urls.add(row['url'])
        
        # Clean the file immediately (removes old errors)
        write_output_file(output_path, successful_rows)
        
        # 2. Get all input rows
        all_input_rows = []
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_input_rows.append(row)
                
        to_process = [r for r in all_input_rows if r.get('url') not in successful_urls]
        print(f"  Already cleaned: {len(successful_rows)}. To process: {len(to_process)}")
        
        for i, row in enumerate(to_process):
            content = (row.get('content') or row.get('summary') or "").strip()
            
            if len(content) < 50:
                row['keywords'] = "Nội dung quá ngắn"
                successful_rows.append(row)
                write_output_file(output_path, successful_rows)
            else:
                success = False
                while not success:
                    for _ in range(len(API_KEYS)):
                        res = generate_for_row(content, key_idx)
                        k_suffix = API_KEYS[key_idx][-5:]
                        key_idx = (key_idx + 1) % len(API_KEYS)
                        
                        if res == "QUOTA":
                            print(f"  [{i+1}/{len(to_process)}] Key ...{k_suffix}: Quota. Trying next...")
                            continue 
                        elif res == "INVALID":
                            continue
                        elif res == "SAFETY":
                            row['keywords'] = "Nội dung nhạy cảm"
                            success = True; break
                        elif res:
                            row['keywords'] = res
                            success = True
                            print(f"  [{i+1}/{len(to_process)}] SUCCESS (Key ...{k_suffix}): {res[:50]}...")
                            break
                        time.sleep(1)
                    
                    if success:
                        successful_rows.append(row)
                        write_output_file(output_path, successful_rows)
                    else:
                        print(f"  [{i+1}/{len(to_process)}] ALL KEYS EXHAUSTED. Sleeping 5 mins to recover...")
                        time.sleep(300)
            
            time.sleep(DELAY_BETWEEN_CALLS)

if __name__ == "__main__":
    main()
