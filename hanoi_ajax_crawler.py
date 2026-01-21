import requests
import re
import csv
import time
import html
from bs4 import BeautifulSoup
from typing import Optional, Dict, List
from urllib.parse import urljoin

# --- CONFIGURATION ---
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://hanoi.gov.vn',
    'Referer': 'https://hanoi.gov.vn/tin-tuc-su-kien-noi-bat'
})

DOMAIN = 'https://hanoi.gov.vn'
ARTICLE_URL_RE = re.compile(r'.*\d+\.htm$', re.IGNORECASE)

API_ENDPOINT = 'https://hanoi.gov.vn/api/NewsZone/NewsZone'
MAX_PAGES = 400
SLEEP_BETWEEN_REQUESTS = 0.5
OUTPUT_FILE = 'hanoi_data_final.csv'

CATEGORIES = {
    'Tin nổi bật': {
        'catname': 'nMSbbZ2pR0/XHR7JMRKsFGiPETTBu6V1',
        'pageSize': 'TwNsaMrfVrU='
    }
}

# --- UTILITIES ---
def clean_text(s: str) -> str:
    if not s:
        return ''
    s = html.unescape(s)
    return re.sub(r'\s+', ' ', s).strip()

def normalize_href(href: str, base: str = DOMAIN) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith(('javascript:', '#', 'mailto:', 'tel:')):
        return None
    if href.startswith('/'):
        return urljoin(DOMAIN, href)
    if href.startswith('http'):
        return href
    return urljoin(base, href)

def check_copyright(soup: BeautifulSoup) -> bool:
    text = soup.get_text().lower()
    has_restrictive = "copy right" in text or "bản quyền thuộc" in text
    has_allowance = "ghi rõ nguồn" in text
    return not (has_restrictive and not has_allowance)

# --- AJAX API ---
def fetch_articles_ajax(catname: str, pageSize: str, page_index: int, data_ids: List[str] = []) -> Optional[Dict]:
    payload = {
        'PageIndex': page_index,
        'PageSize': pageSize,
        'Catname': catname,
        'LanguageId': 'jM2HDDVEz40=',
        'Site': '/CP0MQRJUt0='
    }
    if data_ids:
        payload['DataIds[]'] = data_ids

    try:
        resp = session.post(API_ENDPOINT, data=payload, timeout=20)
        if resp.status_code != 200:
            return None
        return {'data': resp.text}
    except:
        return None

def extract_article_links_from_ajax(ajax_data: Dict) -> List[str]:
    links = []
    soup = BeautifulSoup(ajax_data.get('data', ''), 'html.parser')
    for a in soup.find_all('a', href=True):
        full_url = normalize_href(a['href'])
        if full_url and ARTICLE_URL_RE.search(full_url) and DOMAIN in full_url:
            links.append(full_url)
    return links

# --- ARTICLE PARSER ---
def parse_article(url: str) -> Dict[str, str]:
    result = {
        'title': '',
        'summary': '',
        'content': '',
        'keywords': '',
        'publish_time': '',
        'url': url
    }

    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return result

        soup = BeautifulSoup(resp.text, 'html.parser')

        if not check_copyright(soup):
            return result

        # Title
        if soup.title:
            result['title'] = clean_text(soup.title.string)

        # Summary
        meta_desc = soup.find('meta', property='og:description')
        if meta_desc:
            result['summary'] = clean_text(meta_desc.get('content'))

        if not result['summary']:
            meta_desc_std = soup.find('meta', attrs={'name': 'description'})
            if meta_desc_std:
                result['summary'] = clean_text(meta_desc_std.get('content'))

        # Canonical
        canonical = soup.find('link', rel='canonical')
        if canonical:
            result['url'] = canonical.get('href', url)

        # -------- KEYWORDS (giữ logic + fallback meta) --------
        keywords = []

        keyword_label = soup.find(string=re.compile(r'Từ khóa:', re.IGNORECASE))
        if keyword_label:
            container = keyword_label.parent
            for tag in container.find_all('a'):
                kw = clean_text(tag.get_text())
                if kw:
                    keywords.append(kw)

            if not keywords and container.parent:
                for tag in container.parent.find_all('a'):
                    kw = clean_text(tag.get_text())
                    if kw:
                        keywords.append(kw)

        if not keywords:
            meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
            if meta_keywords:
                content = meta_keywords.get('content', '')
                if content:
                    keywords = [clean_text(k) for k in content.split(',') if k.strip()]

        if keywords:
            result['keywords'] = ', '.join(keywords)
        # -----------------------------------------------------

        # Publish Time
        time_tag = soup.select_one('.news-info .time') or soup.find(class_='detail-time')
        if time_tag:
            result['publish_time'] = clean_text(time_tag.get_text())

        # -------- CONTENT (1 DÒNG DUY NHẤT) --------
        body = soup.select_one('.detail-content.afcbc-body.clearfix')
        if body:
            for junk in body.find_all(['h2', 'img', 'figure', 'figcaption', 'script', 'style']):
                junk.decompose()

            cleaned_paras = []
            paras = body.find_all('p')
            if paras:
                for p in paras:
                    txt = clean_text(p.get_text())
                    if len(txt) > 2:
                        cleaned_paras.append(txt)

                # 👉 CHỈ NẰM TRÊN 1 DÒNG
                result['content'] = ' '.join(cleaned_paras)
            else:
                result['content'] = clean_text(body.get_text())
        # -------------------------------------------

        return result

    except:
        return result

# --- MAIN ---
def main():
    seen_global = set()
    fieldnames = ['topic', 'title', 'summary', 'url', 'keywords', 'public_time', 'content']

    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if row.get('url'):
                    seen_global.add(row['url'])
    except FileNotFoundError:
        pass

    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not seen_global:
            writer.writeheader()

        for topic, cfg in CATEGORIES.items():
            data_ids = []
            for page_index in range(MAX_PAGES):
                ajax = fetch_articles_ajax(cfg['catname'], cfg['pageSize'], page_index, data_ids)
                if not ajax:
                    break

                links = extract_article_links_from_ajax(ajax)
                if not links:
                    break

                for link in links:
                    if link in seen_global:
                        continue
                    seen_global.add(link)

                    data = parse_article(link)
                    if data['title'] and data['content']:
                        writer.writerow({
                            'topic': topic,
                            'title': data['title'],
                            'summary': data['summary'],
                            'url': data['url'],
                            'keywords': data['keywords'],
                            'public_time': data['publish_time'],
                            'content': data['content']
                        })

                data_ids.extend([l.split('/')[-1].replace('.htm', '') for l in links])
                time.sleep(SLEEP_BETWEEN_REQUESTS)

    print("✔ Crawling completed")

if __name__ == "__main__":
    main()
