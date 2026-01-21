
import csv
import os
import sys
import io
import re
from underthesea import pos_tag
from sklearn.feature_extraction.text import TfidfVectorizer

# Force UTF-8 for print statements
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION ---
INPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop\ninhbinh_data_final.csv'
OUTPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed\ninhbinh_data_final_AI_Final.csv'
LIMIT_ROWS = 10  # Output limit
CORPUS_LIMIT = 200 # Read more rows to build better TF-IDF stats (or None for all)

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
    
    # 2. POS Tagging & Filtering (Keep Nouns, Proper Nouns, maybe Verbs)
    try:
        # pos_tag returns list of tuples: [('Hà_Nội', 'Np'), ('là', 'V'), ...]
        tagged_words = pos_tag(text)
        cleaned_words = []
        for word, tag in tagged_words:
            word_lower = word.lower()
            if word_lower in STOP_WORDS:
                continue
            
            # Filter by POS tag: N (Noun), Np (Proper Noun), Nu (Unit Noun), Nc (Classifier Noun)
            # Maybe V (Verb) and A (Adjective) if they are important?
            # Let's focus on Nouns and Proper Nouns for "Entities" and key concepts
            if tag in ['N', 'Np', 'Nu', 'Nc', 'V', 'A']:
                # Remove special chars
                if len(word) > 1 and re.search(r'[a-zA-Z]', word):
                    # Replace underscore with space to allow Tfidf to find 2-3 word phrases from compounds
                    # and also find phrases across tokens
                    cleaned_words.append(word.replace('_', ' '))
    except Exception as e:
        # Fallback
        cleaned_words = text.split()

    return " ".join(cleaned_words)

def process_file():
    print(f"\n--- Processing Ninh Binh Keywords (Global TF-IDF - Phrases) ---")
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found: {INPUT_FILE}")
        return

    # 1. Read Data
    all_rows = []
    with open(INPUT_FILE, 'r', encoding='utf-8-sig') as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            all_rows.append(row)
    
    total_input = len(all_rows)
    print(f"Total input rows: {total_input}")

    # 2. Build Corpus for TF-IDF (using first N rows or all)
    corpus_docs = []
    print("Building corpus and preprocessing...")
    
    # We use more rows for training to get better IDF stats
    training_rows = all_rows[:CORPUS_LIMIT] if CORPUS_LIMIT else all_rows
    
    for row in training_rows:
        text = f"{row.get('title', '')} {row.get('summary', '')} {row.get('content', '')[:3000]}"
        corpus_docs.append(preprocess_text(text))

    # 3. Train TF-IDF
    print("Fitting TF-IDF model...")
    vectorizer = TfidfVectorizer(
        ngram_range=(2, 5), # User requested longer phrases
        max_features=5000,
        min_df=2,
        max_df=0.9,
    )
    
    try:
        tfidf_matrix = vectorizer.fit_transform(corpus_docs)
        feature_names = vectorizer.get_feature_names_out()
    except ValueError:
        print("Error fitting TF-IDF (maybe empty corpus?)")
        return

    # 4. Generate Keywords for Target Rows
    print(f"Generating keywords for first {LIMIT_ROWS} rows...")
    
    if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
        os.makedirs(os.path.dirname(OUTPUT_FILE))

    FIELDNAMES = ['topic', 'title', 'summary', 'url', 'keywords', 'public_time', 'content']
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8-sig', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for i in range(min(LIMIT_ROWS, len(all_rows))):
            row = all_rows[i]
            doc_text = corpus_docs[i] # Already preprocessed
            
            # Calculate TF-IDF for this document against the trained model
            response = vectorizer.transform([doc_text])
            
            # Get top keywords
            scores = response.toarray().flatten()
            ranked_indices = scores.argsort()[::-1]
            
            keywords = []
            seen = set()
            for idx in ranked_indices:
                if scores[idx] < 0.1: # Threshold
                    break
                    
                word = feature_names[idx]
                
                # Filter too short words (length check)
                if len(word) < 4: # Very short words like "ba", "bốn" even if 2-gram? unlikely but good safety
                    continue
                
                if word not in seen:
                    keywords.append(word)
                    seen.add(word)
                
                if len(keywords) >= 5:
                    break
            
            final_keywords = ", ".join(keywords)
            print(f"[{i+1}] {row.get('title', '')[:40]}... -> {final_keywords}")
            
            row['keywords'] = final_keywords
            output_row = {k: row.get(k, '') for k in FIELDNAMES}
            writer.writerow(output_row)

    print(f"\nDone. Output saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    process_file()
