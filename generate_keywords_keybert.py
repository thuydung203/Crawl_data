
import csv
import os
import sys
import io
import re
from keybert import KeyBERT
from underthesea import word_tokenize

# Force UTF-8 for print statements
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION ---
INPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop\ninhbinh_data_final.csv'
OUTPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed\ninhbinh_data_final_AI_Final.csv'
LIMIT_ROWS = 10  # Test run limit

# Comprehensive Vietnamese Stopwords
STOP_WORDS = {
    "là", "và", "của", "thì", "mà", "ở", "những", "cái", "các", "cho", "với", "để", "từ", "sau", "khi",
    "đã", "đang", "sẽ", "được", "bị", "phải", "này", "kia", "đó", "nọ", "ấy", "vậy", "thế", "nào", "gì",
    "ai", "đâu", "khiến", "làm", "nên", "về", "như", "nhưng", "tuy", "bởi", "vì", "do", "bằng", "trong",
    "trên", "dưới", "ngoài", "giữa", "trước", "sau", "tại", "qua", "ra", "vào", "lên", "xuống", "đi",
    "đến", "theo", "cũng", "vẫn", "cứ", "chỉ", "mới", "lại", "rồi", "ngay", "luôn", "từng", "vừa", "cả",
    "bao", "nhiêu", "bấy", "rất", "lắm", "quá", "hơi", "khá", "đủ", "hết", "số", "của", "thuộc", "bà", "ông",
    "anh", "chị", "em", "cô", "chú", "bác", "cháu", "đồng_chí", "ngày", "tháng", "năm", "tỉnh", "huyện", 
    "xã", "phường", "thành_phố", "việt_nam", "ubnd", "ban", "ngành", "sở", "bộ", "công_an", "nhân_dân", 
    "thực_hiện", "triển_khai", "tổ_chức", "tham_gia", "xây_dựng", "phát_triển", "đảm_bảo", "yêu_cầu",
    "nội_dung", "chương_trình", "kế_hoạch", "kết_quả", "công_tác", "hoạt_động", "vấn_đề", "lĩnh_vực",
    "đối_tượng", "hình_thức", "biện_pháp", "giải_pháp", "mục_tiêu", "nhiệm_vụ", "ý_kiến", "chỉ_đạo",
    "kết_luận", "quyết_định", "nghị_định", "thông_tư", "hướng_dẫn", "quy_định", "chính_sách", "hỗ_trợ",
    "dự_án", "công_trình", "đầu_tư", "kinh_tế", "xã_hội", "ninh_bình", "theo", "tại", "việc", 
    "thông_tin", "báo_cáo", "liên_quan", "cụ_thể", "chủ_trương", "địa_phương", "đồng_thời", "tăng_cường"
}

def preprocess_text(text):
    if not text:
        return ""
    # 1. Clean basic noise
    text = re.sub(r'<[^>]*>', '', text) # HTML tags
    text = re.sub(r'http\S+', '', text) # URLs
    text = re.sub(r'\(\s*ảnh[^)]*\)', '', text, flags=re.IGNORECASE) # Remove (Ảnh...)
    
    # 2. KeyBERT handles tokenization internally quite well, but we can help it by removing stopwords
    # Or just passing the raw text. Let's try passing raw text but cleaned of HTML first.
    # The user example code passes 'text' directly.
    return text

def process_file():
    print(f"\n--- Processing Ninh Binh Keywords (KeyBERT) ---")
    print(f"Loading Model: sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 ...")
    
    # Initialize KeyBERT
    kw_model = KeyBERT("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    print("Model loaded successfully.")

    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found: {INPUT_FILE}")
        return

    # 1. Read Data
    rows = []
    with open(INPUT_FILE, 'r', encoding='utf-8-sig') as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            rows.append(row)
    
    total_input = len(rows)
    print(f"Total input rows: {total_input}")

    # 2. Process
    if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
        os.makedirs(os.path.dirname(OUTPUT_FILE))

    FIELDNAMES = ['topic', 'title', 'summary', 'url', 'keywords', 'public_time', 'content']
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8-sig', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for i, row in enumerate(rows):
            if i >= LIMIT_ROWS:
                break
            
            title = row.get('title', '')
            summary = row.get('summary', '')
            content = row.get('content', '')
            
            print(f"[{i+1}] Processing: {title[:50]}...")
            
            # Combine text (Title + Summary + Top Content)
            full_text = f"{title}. {summary}. {content[:3000]}"
            clean_text = preprocess_text(full_text)
            
            try:
                # Extract keywords
                # User suggested: ngram_range=(1, 3), top_n=7
                # We prioritize 2-4 gram phrases as per earlier request for "cụm từ"
                keywords_tuples = kw_model.extract_keywords(
                    clean_text, 
                    keyphrase_ngram_range=(2, 4), 
                    top_n=5,
                    stop_words=list(STOP_WORDS) 
                )
                
                # keywords_tuples is a list of (keyword, score)
                keywords = [kw[0] for kw in keywords_tuples]
                
                final_keywords = ", ".join(keywords)
                print(f"  -> Keywords: {final_keywords}")
                
                row['keywords'] = final_keywords
            except Exception as e:
                print(f"  -> Error extraction: {e}")
                row['keywords'] = ""
            
            output_row = {k: row.get(k, '') for k in FIELDNAMES}
            writer.writerow(output_row)

    print(f"\nDone. Output saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    process_file()
