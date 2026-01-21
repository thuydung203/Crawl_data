
import csv
import os
import sys
import io
import re
import networkx as nx
from underthesea import pos_tag

# Force UTF-8 for print statements
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION ---
INPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop\ninhbinh_data_final.csv'
OUTPUT_FILE = r'c:\Users\Thuyd\Desktop\Downloads\CrawlData\Tong_hop_Processed\ninhbinh_data_final_AI_Final.csv'
LIMIT_ROWS = 10  # Test run limit

# Additional Stopwords
STOP_WORDS.update({
    "có", "hơn", "so", "tính", "việc", "sự", "nhằm", "trên", "dưới", "các", "những", "của", "và", 
    "là", "được", "tại", "trong", "ngoài", "vẫn", "đã", "đang", "như", "khi", "với", "cho", "để", "từ",
    "người", "làm", "thì", "mà", "bị", "bởi", "do", "nên", "về", "theo", "đến", "đi", "ra", "vào",
    "cái", "chiếc", "cuốn", "bức", "tấm", "nhà", "nơi", "chỗ", "phía", "bên", "giữa", "đâu", "nào",
    "ấy", "này", "kia", "đó", "nọ", "gì", "sao", "ai", "tôi", "ta", "họ", "chúng", "mình",
    "khác", "cùng", "nhau", "khiến", "kể", "rất", "quá", "lắm", "thêm", "bớt", "nữa", "mãi",
    "hãy", "chớ", "đừng", "phải", "cần", "muốn", "thích", "yêu", "ghét", "tin", "nghĩ", "biết",
    "thấy", "nhìn", "nghe", "nói", "bảo", "hỏi", "trả_lời", "gọi", "kêu", "hét", "la", "cười",
    "khóc", "ăn", "uống", "ngủ", "nghỉ", "chơi", "học", "làm_việc", "ngồi", "đứng", "đi_lại",
    "chạy", "nhảy", "leo", "trèo", "bơi", "lội", "bay", "lượn", "thăm", "viếng", "gặp", "gỡ",
    "chào", "mừng", "chúc", "tặng", "biếu", "cho_mượn", "vay", "thuê", "mướn", "mua", "bán",
    "trao", "đổi", "lấy", "cầm", "nắm", "giữ", "buông", "thả", "kéo", "đẩy", "mang", "vác",
    "xách", "đội", "đeo", "mặc", "cởi", "tháo", "gỡ", "buộc", "thắt", "kết", "nối", "cắt",
    "xé", "đập", "phá", "hư", "hỏng", "sửa", "chữa", "may", "vá", "thêu", "đan", "dệt",
    "viết", "vẽ", "tô", "xóa", "tẩy", "in", "ấn", "phát", "chiếu", "xem", "đọc", "nghe", 
    "nhìn", "thấy", "biết", "hiểu", "nhớ", "quên", "yêu", "ghét", "thương", "giận", "vui",
    "buồn", "lo", "sợ", "ngại", "tiếc", "phiền", "bực", "tức", "căm", "thù", "hờn", "dỗi",
    "ách", "bè", "hội", "nhóm", "phe", "đảng", "phái", "chi", "nhánh", "ngành", "lớp",
    "tầng", "cấp", "bậc", "hạng", "loại", "kiểu", "mẫu", "dạng", "hình", "thức", "thể",
    "cách", "lối", "phép", "tắc", "luật", "lệ", "quy", "định", "chế", "độ", "chính", "sách",
    "chương", "trình", "kế", "hoạch", "dự", "án", "công", "trình", "mục", "tiêu", "ý", "nghĩa",
    "giá", "trị", "kết", "quả", "hậu", "quả", "nguyên", "nhân", "lý", "do", "cơ", "sở",
    "nền", "tảng", "gốc", "ngọn", "đầu", "đuôi", "giữa", "trung", "tâm", "đỉnh", "đáy",
    "cạnh", "góc", "mặt", "trái", "phải", "trên", "dưới", "trong", "ngoài", "trước", "sau",
    "đông", "tây", "nam", "bắc", "xuân", "hạ", "thu", "đông", "sáng", "trưa", "chiều", "tối",
    "đêm", "ngày", "tháng", "năm", "giờ", "phút", "giây", "tuần", "quý", "kỳ", "đợt",
    "khóa", "buổi", "phiên", "lần", "lượt", "chuyến", "vòng", "lượt", "chặng", "giai", "đoạn",
    "thời", "gian", "khoảng", "khắc", "lát", "chốc", "bữa", "hôm", "mai", "mot", 
    "xưa", "nay", "giờ", "sau"
})

def extract_keywords_textrank(text, top_n=5, window_size=3):
    if not text:
        return ""
    
    # 1. POS Tagging
    try:
        tagged_words = pos_tag(text)
    except:
        return ""

    # 2. Build Graph (Words)
    allowed_pos = {'N', 'Np', 'Nu', 'Nc', 'Ny', 'A', 'V'} 
    
    # Filter for graph nodes
    relevant_tokens = []
    for word, tag in tagged_words:
        word_lower = word.lower()
        if word_lower not in STOP_WORDS and len(word) > 1 and (word.isalpha() or '_' in word):
             if tag in allowed_pos:
                relevant_tokens.append(word_lower)
    
    # Build Graph
    graph = nx.Graph()
    graph.add_nodes_from(relevant_tokens)
    
    length = len(relevant_tokens)
    for i in range(length):
        for j in range(i + 1, min(i + window_size, length)):
            w1 = relevant_tokens[i]
            w2 = relevant_tokens[j]
            if w1 != w2:
                if graph.has_edge(w1, w2):
                    graph[w1][w2]['weight'] += 1
                else:
                    graph.add_edge(w1, w2, weight=1)
                    
    # Compute PageRank (Word Scores)
    if len(graph.nodes) == 0:
        return ""
    try:
        word_scores = nx.pagerank(graph, weight='weight')
    except:
        return ""

    # 3. Extract Candidates (Phrases) from Original Text
    candidates = {}
    current_phrase = []
    
    for word, tag in tagged_words:
        word_label = word.lower()
        
        # Must be allowed POS and NOT a stopword to start/continue a phrase
        is_content = (tag in allowed_pos) and (word_label not in STOP_WORDS) and (len(word) > 1) and (word.isalpha() or '_' in word)
        
        if is_content:
            current_phrase.append(word_label)
        else:
            if len(current_phrase) >= 2:
                # Sliding window for 2-3 grams
                chunk_len = len(current_phrase)
                for n in range(2, 4):
                    for i in range(chunk_len - n + 1):
                        gram = current_phrase[i : i+n]
                        
                        # Check start/end words -> Should NOT be stopwords (already filtered by is_content loop logic mostly, but double check)
                        if gram[0] in STOP_WORDS or gram[-1] in STOP_WORDS:
                            continue
                            
                        # Calculate score
                        score = 0
                        for w in gram:
                            if w in word_scores:
                                score += word_scores[w]
                        
                        if score > 0:
                             phrase_str = " ".join(gram).replace('_', ' ')
                             candidates[phrase_str] = score
            
            current_phrase = []

    # Single keywords (Compounds only)
    for w in word_scores:
        if '_' in w and w not in STOP_WORDS: 
             phrase_str = w.replace('_', ' ')
             candidates[phrase_str] = word_scores[w]
    
    # 4. Sort Candidates & Deduplicate
    sorted_phrases = sorted(candidates.items(), key=lambda x: x[1], reverse=True)
    
    final_keywords = []
    seen = set()
    
    for phrase, score in sorted_phrases:
        if len(final_keywords) >= top_n:
            break
        
        # Deduplication logic
        is_dup = False
        for s in seen:
            if phrase in s or s in phrase: # Basic substring check
                # Prefer the longer one? or shorter?
                # Usually we want the longer, more specific one if scores are similar.
                # If we already have "kinh tế", and "phát triển kinh tế" comes up?
                # Since we sort by score, highest score comes first.
                # If "kinh tế" has higher score (sum of 2 words) vs "phát triển kinh tế" (sum of 3), usually 3 has higher score.
                # So "phát triển kinh tế" comes first. Then "kinh tế" comes. We should skip "kinh tế".
                if phrase in s: # current is substring of seen -> Skip
                     is_dup = True
                     break
                # What if the NEW one is superstring of seen? (e.g. seen "lúa", new "giống lúa")
                # But we are iterating by Score. so if "lúa" came first, it had higher score? Unlikely for sum-scoring.
                # sum("giống lúa") > sum("lúa") usually.
                # So mostly we see Long phrases first.
                pass
        
        if not is_dup:
            final_keywords.append(phrase)
            seen.add(phrase)

    return ", ".join(final_keywords)

def process_file():
    print(f"\n--- Processing Ninh Binh Keywords (TextRank + POS Filter) ---")
    
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
            
            # Combine text (Title + Summary + Content)
            full_text = f"{title}. {summary}. {content[:5000]}"
            clean_text = preprocess_text(full_text)
            
            try:
                # Extract keywords
                keywords = extract_keywords_textrank(clean_text, top_n=5)
                print(f"  -> Keywords: {keywords}")
                row['keywords'] = keywords
            except Exception as e:
                print(f"  -> Error extraction: {e}")
                row['keywords'] = ""
            
            output_row = {k: row.get(k, '') for k in FIELDNAMES}
            writer.writerow(output_row)

    print(f"\nDone. Output saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    process_file()
