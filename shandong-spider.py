import requests
import re
import json
import hashlib
import time
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup

OUTPUT_FILE = "shandong_pv_data.json"
MAX_PAGES = 5
REQUEST_DELAY = 1

SOURCES = [
    {"name": "山东能源监管办", "category": "policy", "url": "https://sdb.nea.gov.cn/dtyw/jgdt/", "base": "https://sdb.nea.gov.cn", "selector": "ul.list li a", "keywords": ["光伏", "分布式", "集中式"], "date_pattern": r"(\d{4}-\d{2}-\d{2})", "next_page": "a.next"},
    {"name": "山东省能源局", "category": "policy", "url": "http://nyj.shandong.gov.cn/col/col100099/index.html", "base": "http://nyj.shandong.gov.cn", "selector": "ul.list li a", "keywords": ["光伏", "分布式", "竞价"], "date_pattern": r"(\d{4}-\d{2}-\d{2})", "next_page": "a:contains('下一页')"},
    {"name": "国家能源局", "category": "policy", "url": "https://www.nea.gov.cn/search/searchZcWj.htm?keywords=山东+光伏", "base": "https://www.nea.gov.cn", "selector": "div.search-res-list a", "keywords": ["光伏", "山东"], "date_pattern": r"(\d{4}-\d{2}-\d{2})", "next_page": "a.next"},
    {"name": "北极星光伏网-山东市场", "category": "market", "url": "https://guangfu.bjx.com.cn/Search/Index?keyword=山东+市场", "base": "https://guangfu.bjx.com.cn", "selector": "ul.list li a", "keywords": ["市场", "装机", "分析"], "date_pattern": None, "next_page": "a.nextPage"},
    {"name": "国网山东电力电商平台", "category": "project", "url": "https://ecp.sgcc.com.cn/ecp2.0/portal/#/list/list-solicit", "base": "https://ecp.sgcc.com.cn", "selector": "div.solicit-info a", "keywords": ["光伏", "EPC", "组件"], "date_pattern": None, "next_page": None},
    {"name": "中国招标投标公共服务平台", "category": "project", "url": "https://bulletin.cebpubservice.com/search/advanced?searchValue=光伏&province=山东", "base": "https://bulletin.cebpubservice.com", "selector": "div.bid-info a", "keywords": ["光伏", "EPC", "采购"], "date_pattern": r"(\d{4}-\d{2}-\d{2})", "next_page": "a.next"},
    {"name": "山东政府采购网", "category": "project", "url": "http://www.ccgp-shandong.gov.cn/sdgp2017/site/list.jsp?classid=98&searchText=%E5%85%89%E4%BC%8F", "base": "http://www.ccgp-shandong.gov.cn", "selector": "table tr a", "keywords": ["光伏"], "date_pattern": None, "next_page": "a:contains('下一页')"},
    {"name": "中能建电子采购平台", "category": "project", "url": "https://ec.ceec.net.cn/HomeInfo/ProjectList.aspx?type=招标&area=山东&keyword=光伏", "base": "https://ec.ceec.net.cn", "selector": "div.project-item a", "keywords": ["光伏"], "date_pattern": None, "next_page": "a.next"},
    {"name": "中国电建集采平台", "category": "project", "url": "https://ec.powerchina.cn/eb2/notice/list?searchValue=光伏", "base": "https://ec.powerchina.cn", "selector": "li.clearfix a", "keywords": ["光伏", "组件"], "date_pattern": None, "next_page": "a.next"},
    {"name": "济南公共资源交易中心", "category": "project", "url": "http://jnggzy.jinan.gov.cn/jyxx/005/005001/moreinfo2.html", "base": "http://jnggzy.jinan.gov.cn", "selector": "ul.list a", "keywords": ["光伏"], "date_pattern": r"(\d{4}-\d{2}-\d{2})", "next_page": "a.next"},
    {"name": "青岛公共资源交易中心", "category": "project", "url": "https://ggzy.qingdao.gov.cn/PortalQD/InfoMore?classifyId=010101", "base": "https://ggzy.qingdao.gov.cn", "selector": "div.info-item a", "keywords": ["光伏"], "date_pattern": None, "next_page": "a.next"},
    {"name": "山东备案系统公示", "category": "project", "url": "http://www.shandong.gov.cn/col/col94091/", "base": "http://www.shandong.gov.cn", "selector": "div.list-box a", "keywords": ["备案", "光伏"], "date_pattern": r"(\d{4}-\d{2}-\d{2})", "next_page": "a.next"},
]

def load_existing():
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            fingerprints = set()
            for item in data:
                fp = hashlib.md5(f"{item['title']}{item['source']}{item.get('publish_date','')}".encode()).hexdigest()
                fingerprints.add(fp)
            return data, fingerprints
    except:
        return [], set()

def extract_title_from_detail(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            title = title_tag.string.strip()
            for suffix in ["- 山东省能源局", "- 山东能源监管办", "- 国家能源局", "| 国家电网", "_ 招标公告"]:
                if title.endswith(suffix):
                    title = title[:-len(suffix)].strip()
            return title
    except:
        pass
    return None

def extract_date_from_html(html):
    patterns = [r"(\d{4}-\d{2}-\d{2})", r"(\d{4}年\d{1,2}月\d{1,2}日)"]
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            date_str = m.group(1)
            if '年' in date_str:
                date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
            return date_str
    return datetime.now().strftime("%Y-%m-%d")

def fetch_page(source, url, page_num):
    new_items = []
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = soup.select(source["selector"])
        for link in links:
            list_title = link.get_text(strip=True)
            href = link.get('href')
            if not href or not list_title:
                continue
            if not any(kw in list_title for kw in source.get("keywords", ["光伏"])):
                continue
            full_url = urljoin(source["base"], href).split('#')[0]
            try:
                detail_resp = requests.get(full_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                detail_resp.encoding = 'utf-8'
                detail_html = detail_resp.text
                real_title = extract_title_from_detail(detail_html)
                if not real_title:
                    real_title = list_title
                pub_date = ""
                if source.get("date_pattern"):
                    parent = link.find_parent()
                    if parent:
                        match = re.search(source["date_pattern"], parent.get_text())
                        if match:
                            pub_date = match.group(1)
                if not pub_date:
                    pub_date = extract_date_from_html(detail_html)
                soup_detail = BeautifulSoup(detail_html, 'html.parser')
                for script in soup_detail(["script", "style"]):
                    script.decompose()
                text = soup_detail.get_text()
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                content = ' '.join(chunk for chunk in chunks if chunk)
                if len(content) > 2000:
                    content = content[:2000] + "...\n（内容过长，已截断）"
            except Exception as e:
                print(f"    详情页处理失败 {full_url}: {e}")
                real_title = list_title
                pub_date = datetime.now().strftime("%Y-%m-%d")
                content = f"（原文链接：{full_url}）"
            new_items.append({
                "id": hashlib.md5(f"{real_title}{source['name']}{pub_date}".encode()).hexdigest()[:16],
                "title": real_title[:200],
                "content": content,
                "category": source["category"],
                "source": source["name"],
                "publish_date": pub_date,
                "url": full_url
            })
        next_link = None
        if source.get("next_page"):
            next_elem = soup.select_one(source["next_page"])
            if next_elem and next_elem.get('href'):
                next_link = urljoin(source["base"], next_elem['href'])
        return new_items, next_link
    except Exception as e:
        print(f"  页 {url} 抓取失败: {e}")
        return [], None

def fetch_source(source):
    all_items = []
    url = source["url"]
    for page in range(1, MAX_PAGES + 1):
        print(f"  第{page}页: {url}")
        items, next_url = fetch_page(source, url, page)
        all_items.extend(items)
        if not next_url:
            break
        url = next_url
        time.sleep(REQUEST_DELAY)
    return all_items

def main():
    print("启动山东光伏爬虫（每日运行，增量追加，标题取自详情页）")
    existing_list, fp_set = load_existing()
    new_records = []
    for src in SOURCES:
        print(f"处理: {src['name']}")
        records = fetch_source(src)
        for rec in records:
            fp = hashlib.md5(f"{rec['title']}{rec['source']}{rec['publish_date']}".encode()).hexdigest()
            if fp not in fp_set:
                fp_set.add(fp)
                new_records.append(rec)
                print(f"  ✅ 新增 [{rec['category']}] {rec['title'][:40]}...")
    all_data = existing_list + new_records
    all_data.sort(key=lambda x: x.get('publish_date', ''), reverse=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    print(f"完成：原有 {len(existing_list)}，新增 {len(new_records)}，总计 {len(all_data)} 条")

if __name__ == "__main__":
    main()