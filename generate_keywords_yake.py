
import csv
import os
import sys
import io
import re
import yake
from underthesea import pos_tag

# Force UTF-8 for print statements
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION ---
INPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop'
OUTPUT_DIR = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed'
# LIMIT_ROWS = 10 # Commented out for full run

# --- USER PROVIDED LOGIC ---
kw_extractor = yake.KeywordExtractor(
    lan="vi",
    n=3,
    top=30
)

STOP_PREFIX = ["mức", "đạt", "tăng", "cao", "nhiều", "các", "những", "của", "và", "là", "được", "tại", "trong"] # Expanded slightly for safety

def clean_text(text):
    text = text.lower()
    text = re.sub(r'<[^>]*>', ' ', text) # Added basic HTML cleaning
    text = re.sub(r'http\S+', '', text) # Added URL cleaning
    text = re.sub(r'\d+', " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def normalize_phrase(p):
    words = p.split()
    # bỏ cụm quá chung chung
    if not words: return None
    if words[0] in STOP_PREFIX:
        return None
    return " ".join(words)

def is_duplicate(phrase, selected):
    for s in selected:
        # trùng từ gốc
        if phrase in s or s in phrase:
            return True
        # trùng quá 70% từ
        overlap = set(phrase.split()) & set(s.split())
        if len(overlap) / max(len(phrase.split()), 1) > 0.7:
            return True
    return False

def extract_keywords(text, top_n=6):
    text = clean_text(text)
    if not text: return ""
    
    candidates = kw_extractor.extract_keywords(text)

    final = []
    # YAKE returns (keyword, score) where LOWER score is better. 
    # But candidates list is usually sorted by score already? 
    # Yes, YAKE returns sorted list.
    
    for kw, score in candidates:
        kw = normalize_phrase(kw)
        if not kw:
            continue

        words = kw.split()
        if len(words) < 2:
            continue

        try:
            tags = pos_tag(kw)
        except:
            continue
            
        # User requested: all(t[1].startswith("N") for t in tags)
        # This is very strict. "phát triển kinh tế" -> "phát triển" is V.
        # But user said "Chỉ lấy Danh từ / Cụm danh từ".
        # Let's stick to their rule.
        if not all(t[1].startswith("N") for t in tags):
            continue

        if is_duplicate(kw, final):
            continue

        final.append(kw)

        if len(final) >= top_n:
            break

    return ", ".join(final)

def process_single_file(filename):
    input_path = os.path.join(INPUT_DIR, filename)
    
    # Create output filename: name.csv -> name_AI_Final.csv
    base_name = os.path.splitext(filename)[0]
    output_filename = f"{base_name}_AI_Final.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    
    print(f"\n>>> Processing: {filename} -> {output_filename}")
    
    if not os.path.exists(input_path):
        print(f"Error: Input file not found: {input_path}")
        return

    # 1. Read Data
    rows = []
    try:
        with open(input_path, 'r', encoding='utf-8-sig') as f_in:
            reader = csv.DictReader(f_in)
            if not reader.fieldnames:
                print(f"  Skipping {filename}: Empty or invalid CSV.")
                return
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"  Error reading {filename}: {e}")
        return
    
    total_input = len(rows)
    print(f"  Total rows: {total_input}")

    # 2. Process
    FIELDNAMES = ['topic', 'title', 'summary', 'url', 'keywords', 'public_time', 'content']
    
    with open(output_path, 'w', encoding='utf-8-sig', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for i, row in enumerate(rows):
            # if i >= LIMIT_ROWS: break # Uncomment for testing
            
            title = row.get('title', '')
            summary = row.get('summary', '')
            content = row.get('content', '')
            
            # Combine text (Title + Summary + Content)
            full_text = f"{title}. {summary}. {content}"
            if len(full_text) > 20000: # Truncate massive content for speed/memory if needed
                 full_text = full_text[:20000]
            
            try:
                keywords = extract_keywords(full_text, top_n=6)
                row['keywords'] = keywords
            except Exception as e:
                # print(f"  -> Error extraction: {e}")
                row['keywords'] = ""
                
            if (i + 1) % 100 == 0:
                print(f"  Processed {i + 1}/{total_input} rows...")
            
            output_row = {k: row.get(k, '') for k in FIELDNAMES}
            writer.writerow(output_row)

    print(f"  Done. Saved to {output_path}")

def process_all_files():
    print(f"\n--- Batch Processing Keywords (YAKE + POS) ---")
    
    if not os.path.exists(INPUT_DIR):
        print(f"Input directory not found: {INPUT_DIR}")
        return
        
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith('.csv')]
    
    for f in files:
        # Skip backups or temp files if needed, but User requested "all crawled CSV files"
        # We might want to skip files that already look processed?
        if "_AI_Final" in f:
            continue
            
        process_single_file(f)

if __name__ == "__main__":
    process_all_files()
