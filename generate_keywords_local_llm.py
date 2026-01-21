
import pandas as pd
import csv
import os
import sys
import io
from llama_cpp import Llama

# Force UTF-8 for print statements
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION ---
MODEL_PATH = "models/qwen2.5-3b-instruct-q4_k_m.gguf"
INPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop'
OUTPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed'

# Create output directory if needed
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# --- LOAD MODEL (ONCE) ---
print(f"Loading model from {MODEL_PATH}...")
try:
    llm = Llama(
        model_path=MODEL_PATH,
        n_ctx=4096,      # Context window
        n_threads=6,     # CPU threads
        verbose=False    # Suppress heavy logs
    )
    print("Model loaded successfully!")
except Exception as e:
    print(f"FATAL ERROR: Could not load model. {e}")
    sys.exit(1)

# --- PROMPT TEMPLATE ---
PROMPT_TEMPLATE = """Bạn là chuyên gia trích xuất từ khóa cho báo chí Việt Nam.

Nhiệm vụ: Trích xuất 4-7 từ khóa từ nội dung bên dưới.

Quy tắc BẮT BUỘC:
1. Mỗi từ khóa phải là CỤM DANH TỪ HOÀN CHỈNH (ví dụ: "phát triển kinh tế", "chuyển đổi số")
2. KHÔNG được dùng từ đơn lẻ
3. KHÔNG được lặp lại
4. KHÔNG được viết câu dài
5. Chỉ trả về ĐÚNG 1 DÒNG duy nhất

Định dạng output (QUAN TRỌNG):
Chỉ trả về các từ khóa cách nhau bởi dấu phẩy, KHÔNG có "key1", "key2", KHÔNG có số thứ tự.

Ví dụ đúng: phát triển kinh tế, chuyển đổi số, nông nghiệp công nghệ cao

Nội dung cần trích xuất:
{content}

Từ khóa:"""

def generate_keywords(text):
    if not text or len(str(text)) < 50:
        return ""
    
    # Truncate content to fit context (approx 3000 chars ~ 750-1000 tokens)
    # Check max context of model (4096), keep prompt safe.
    prompt = PROMPT_TEMPLATE.format(content=text[:3000])
    
    try:
        output = llm(
            prompt, 
            max_tokens=128, # Short output for keywords
            stop=["\n", "Nội dung", "YÊU CẦU", "Quy tắc"], # Stop tokens
            temperature=0.2, # Very low temp for deterministic output
            top_p=0.9
        )
        result = output["choices"][0]["text"].strip()
        
        # Clean up the output
        # Remove any "key1:", "key2:" patterns
        import re
        result = re.sub(r'key\d+:\s*', '', result, flags=re.IGNORECASE)
        # Remove quotes
        result = result.replace('"', '').replace("'", "")
        # Remove any remaining numbering like "1.", "2."
        result = re.sub(r'^\d+\.\s*', '', result)
        result = re.sub(r',\s*\d+\.\s*', ', ', result)
        
        # ENFORCE 4-7 KEYWORD LIMIT
        keywords = [kw.strip() for kw in result.split(',') if kw.strip()]
        # Take only first 7 keywords
        keywords = keywords[:7]
        # If less than 4, keep what we have
        result = ', '.join(keywords)
        
        return result.strip()
    except Exception as e:
        print(f"Error generating keywords: {e}")
        return ""

def process_file(filename):
    input_path = os.path.join(INPUT_DIR, filename)
    base_name = os.path.splitext(filename)[0]
    output_filename = f"{base_name}_AI_Final.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    
    print(f"\n>>> Processing: {filename} -> {output_filename}")
    
    if not os.path.exists(input_path):
        print(f"Error: Not found {input_path}")
        return

    # Read Data
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
         print(f"Error reading CSV {filename}: {e}")
         return

    total = len(df)
    print(f"  Total rows: {total}")
    
    # Prepare output columns
    FIELDNAMES = ['topic', 'title', 'summary', 'url', 'keywords', 'public_time', 'content']
    
    # Check if output file already exists to resume
    start_row = 0
    if os.path.exists(output_path):
        try:
            existing_df = pd.read_csv(output_path)
            start_row = len(existing_df)
            print(f"  Resuming from row {start_row} (found existing output)")
        except:
            start_row = 0
    
    # Open output file in append mode (or write mode if starting fresh)
    mode = 'a' if start_row > 0 else 'w'
    write_header = (start_row == 0)
    
    with open(output_path, mode, encoding='utf-8-sig', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
        
        if write_header:
            writer.writeheader()
        
        # Process each row starting from start_row
        for i in range(start_row, total):
            row = df.iloc[i]
            
            if (i + 1) % 50 == 0 or i == start_row:
                print(f"    Processed {i + 1}/{total} rows...")
            
            # Combine text for keyword generation
            title = str(row.get('title', ''))
            summary = str(row.get('summary', ''))
            content = str(row.get('content', ''))
            full_text = f"{title}. {summary}. {content[:5000]}"
            
            try:
                # Generate keywords
                keywords = generate_keywords(full_text)
            except Exception as e:
                print(f"  -> Error at row {i+1}: {e}")
                keywords = ""
            
            # Prepare output row
            output_row = {}
            for col in FIELDNAMES:
                if col == 'keywords':
                    output_row[col] = keywords
                else:
                    output_row[col] = row.get(col, '')
            
            # Write immediately (incremental save)
            writer.writerow(output_row)
            f_out.flush()  # Force write to disk
    
    print(f"  Done. Saved to {output_path}")

def main():
    if not os.path.exists("models"):
        os.makedirs("models")
        
    if not os.path.exists(MODEL_PATH):
        print(f"WARNING: Model file not found at {MODEL_PATH}")
        print("Please ensure you have downloaded 'qwen2.5-3b-instruct-q4_k_m.gguf' into 'models/' folder.")
        return

    # List all CSVs
    all_files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith('.csv')]
    
    # Skip files that user wants to handle separately
    skip_files = ["bacgiang_data_final.csv"]
    
    # Prioritize Ninh Binh first
    priority_file = "ninhbinh_data_final.csv"
    files = []
    if priority_file in all_files and priority_file not in skip_files:
        files.append(priority_file)
    
    # Then add remaining files
    for f in all_files:
        if f != priority_file and "_AI_Final" not in f and f not in skip_files:
            files.append(f)
    
    print(f"\nTotal files to process: {len(files)}")
    print(f"Skipping: {', '.join(skip_files)}")
    
    for f in files:
        process_file(f)

if __name__ == "__main__":
    main()
