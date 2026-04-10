import requests
import json
import time
import os
import webbrowser
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── 1. 配置 ────────────────────────────────────────────────────────────────────

ETF_MAP = {
    '510300': '华泰柏瑞沪深300ETF',
    '510310': '易方达沪深300ETF',
    '510330': '华夏沪深300ETF',
    '510050': '华夏上证50ETF',
    '510500': '南方中证500ETF',
    '512100': '南方中证1000ETF',
    '510180': '华安上证180ETF',
    '560010': '广发中证1000ETF',
    '588080': '易方达上证科创板50ETF',
}

TARGET_DAYS      = 1520
CUTOFF_DATE      = datetime(2020, 1, 1)
OUTPUT_HTML      = 'sse_final_dashboard.html'
OUTPUT_EXCEL     = 'sse_etf_data.xlsx'
CHECKPOINT       = 'sse_checkpoint.json'
MAX_NET_FAILURES = 5
NET_RETRY_WAIT   = 3

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer':    'https://www.sse.com.cn/',
}

# ── 2. 数据抓取增强 (新增上证指数) ─────────────────────────────────────────────

def fetch_sse_index(start_date, end_date):
    """从搜狐获取上证指数历史数据"""
    s = start_date.replace('-', '')
    e = end_date.replace('-', '')
    url = f"https://q.stock.sohu.com/hisHq?code=zs_000001&start={s}&end={e}"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if isinstance(data, list) and len(data) > 0 and 'hq' in data[0]:
            # 返回格式: [[日期, 开盘, 收盘, 涨跌...], ...]
            return {item[0]: float(item[2]) for item in data[0]['hq']}
    except Exception as ex:
        print(f"⚠️ 获取上证指数失败: {ex}")
    return {}

def fetch_day_etf(date_str):
    ts = int(time.time() * 1000)
    url = f'https://query.sse.com.cn/commonQuery.do?isPagination=true&pageHelp.pageSize=1000&sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE={date_str}&_{ts}'
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        items = resp.json().get('pageHelp', {}).get('data', [])
        return items, 'ok' if items else 'nodata'
    except:
        return None, 'neterr'

# ── 3. 断点工具 (支持指数保存) ────────────────────────────────────────────────

def load_checkpoint():
    if not os.path.exists(CHECKPOINT): return [], {}, None
    try:
        with open(CHECKPOINT, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('results', []), data.get('index_data', {}), data.get('last_date', None)
    except: return [], {}, None

def save_checkpoint(results, index_data, note=''):
    if not results: return
    dates = [r['date'] for r in results]
    tmp = CHECKPOINT + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({
                'last_date': min(dates),
                'first_date': max(dates),
                'index_data': index_data,
                'results': results,
                'note': note
            }, f, ensure_ascii=False)
        os.replace(tmp, CHECKPOINT)
    except KeyboardInterrupt:
        if os.path.exists(tmp): os.replace(tmp, CHECKPOINT)
        raise

# ── 4. 业务逻辑 ────────────────────────────────────────────────────────────────

def sync_data():
    results, index_data, last_date = load_checkpoint()
    existing_dates = {r['date'] for r in results}
    
    # 计算需要抓取的日期范围
    start_dt = datetime.strptime(last_date, '%Y-%m-%d') - timedelta(days=1) if last_date else datetime.today()
    date_list = []
    curr = start_dt
    while curr >= CUTOFF_DATE and len(existing_dates) + len(date_list) < TARGET_DAYS:
        ds = curr.strftime('%Y-%m-%d')
        if ds not in existing_dates: date_list.append(ds)
        curr -= timedelta(days=1)

    # 抓取 ETF 数据
    new_recs = []
    try:
        for i, ds in enumerate(date_list):
            print(f'同步 {ds} [ETF数据]', end='  ')
            items, status = fetch_day_etf(ds)
            if status == 'ok':
                new_recs.append({'date': ds, 'items': items})
                print('✓')
                if i % 5 == 0: save_checkpoint(results + new_recs, index_data, '同步中')
            else: print('—')
            time.sleep(0.3)
    except KeyboardInterrupt: print('\n⏸ ETF同步中断')
    
    results.extend(new_recs)
    results.sort(key=lambda x: x['date'], reverse=True)

    # 增量抓取上证指数
    if results:
        latest, earliest = results[0]['date'], results[-1]['date']
        print(f"🔄 正在补全上证指数历史 ({earliest} ~ {latest})...")
        new_index = fetch_sse_index(earliest, latest)
        index_data.update(new_index)
        save_checkpoint(results, index_data, '全量完成')
    
    return results, index_data

# ── 5. Excel 导出 (含指数) ────────────────────────────────────────────────────

def generate_excel(results, index_data, output_path):
    wb = Workbook()
    ws1 = wb.active
    ws1.title = '规模及指数'
    
    # 提取所有 ETF 数据
    sample = results[0]['items'][0]
    c_key = next((k for k in sample.keys() if str(sample[k]).strip()[:2] in ('51','56','58')), 'SEC_CODE')
    v_key = next((k for k in sample.keys() if any(kw in k.upper() for kw in ('VOL','SHARE','份额'))), None)
    
    # 表头
    headers = ['日期', '上证指数'] + [f"{v}({k})" for k,v in ETF_MAP.items()]
    for ci, h in enumerate(headers, 1):
        cell = ws1.cell(1, ci, h)
        cell.font = Font(bold=True, color='FFFFFF')
        cell.fill = PatternFill('solid', start_color='1F4E79')

    # 数据行
    for ri, day in enumerate(sorted(results, key=lambda x: x['date']), 2):
        ds = day['date']
        ws1.cell(ri, 1, ds)
        ws1.cell(ri, 2, index_data.get(ds))
        for ci, code in enumerate(ETF_MAP.keys(), 3):
            item = next((i for i in day['items'] if str(i.get(c_key, '')).strip() == code), None)
            val = float(str(item[v_key]).replace(',', '')) if item and item.get(v_key) else None
            ws1.cell(ri, ci, val)

    wb.save(output_path)
    print(f'✅ Excel 已保存至: {output_path}')

# ── 6. HTML 模板 (双 Y 轴修复) ───────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8"><title>ETF监控(含上证指数)</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body { background:#f5f7fa; font-family:sans-serif; padding:20px; }
        .tabs { display:flex; flex-wrap:wrap; gap:8px; justify-content:center; margin-bottom:15px; }
        .tab-btn { padding:6px 12px; background:#fff; border:1px solid #ddd; border-radius:4px; cursor:pointer; font-size:13px; }
        .tab-btn.active { background:#409EFF; color:#fff; border-color:#409EFF; }
        .chart-box { background:#fff; border-radius:8px; padding:15px; box-shadow:0 2px 10px rgba(0,0,0,0.05); height:700px; }
    </style>
</head>
<body>
    <h2 style="text-align:center">上交所宽基 ETF 规模 vs 上证指数</h2>
    <div class="tabs" id="tabs"></div>
    <div id="chart" class="chart-box"></div>
    <script>
        const etfData = PLOT_DATA_JSON;
        const indexTrace = INDEX_TRACE_JSON;
        
        const layout = {
            hovermode:'x unified',
            xaxis: { type:'category', tickangle:-45, dtick:30 },
            yaxis: { title:'ETF规模 (万份)', side:'left' },
            yaxis2: { title:'上证指数', side:'right', overlaying:'y', showgrid:false, zeroline:false },
            legend: { orientation:'h', y:-0.2 },
            margin: { b:100, r:80 }
        };

        function render(traceIndices) {
            const visibleTraces = etfData.filter((_, i) => traceIndices.includes(i));
            // 指数始终置于最后，并绑定 yaxis2
            const tracesToShow = [...visibleTraces, {...indexTrace, yaxis:'y2'}];
            Plotly.newPlot('chart', tracesToShow, layout);
        }

        const tabs = document.getElementById('tabs');
        const createBtn = (text, onClick, active=false) => {
            const btn = document.createElement('div');
            btn.className = 'tab-btn' + (active?' active':'');
            btn.textContent = text;
            btn.onclick = function() {
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                onClick();
            };
            tabs.appendChild(btn);
        };

        createBtn('📊 总体汇总', () => render(etfData.map((_,i)=>i)), true);
        etfData.forEach((t, i) => createBtn(t.name.split('(')[0], () => render([i])));
        
        render(etfData.map((_,i)=>i));
    </script>
</body>
</html>"""

def generate_html(results, index_data, output_path):
    # 构建 ETF traces
    sample = results[0]['items'][0]
    c_key = next((k for k in sample.keys() if str(sample[k]).strip()[:2] in ('51','56','58')), 'SEC_CODE')
    v_key = next((k for k in sample.keys() if any(kw in k.upper() for kw in ('VOL','SHARE','份额'))), None)
    
    all_etf_traces = []
    for code, name in ETF_MAP.items():
        pairs = []
        for day in sorted(results, key=lambda x: x['date']):
            ds = day['date']
            item = next((i for i in day['items'] if str(i.get(c_key, '')).strip() == code), None)
            val = float(str(item[v_key]).replace(',', '')) if item and item.get(v_key) else None
            pairs.append((ds, val))
        all_etf_traces.append({
            'x': [p[0] for p in pairs], 'y': [p[1] for p in pairs],
            'name': f'{name}({code})', 'mode': 'lines', 'line': {'width': 2}
        })

    # 构建指数 trace
    sorted_days = sorted(results, key=lambda x: x['date'])
    index_trace = {
        'x': [d['date'] for d in sorted_days],
        'y': [index_data.get(d['date']) for d in sorted_days],
        'name': '上证指数 (右轴)', 'line': {'dash': 'dot', 'color': '#999', 'width': 3},
        'opacity': 0.6
    }

    html = HTML_TEMPLATE.replace('PLOT_DATA_JSON', json.dumps(all_etf_traces, ensure_ascii=False))
    html = html.replace('INDEX_TRACE_JSON', json.dumps(index_trace, ensure_ascii=False))
    
    with open(output_path, 'w', encoding='utf-8') as f: f.write(html)
    print(f'✅ HTML 已生成: {output_path}')

# ── 7. 主程序 ──────────────────────────────────────────────────────────────────

def main():
    print("="*50 + "\n上交所 ETF 规模 & 上证指数 监控系统\n" + "="*50)
    results, index_data = sync_data()
    
    if results:
        generate_excel(results, index_data, OUTPUT_EXCEL)
        generate_html(results, index_data, OUTPUT_HTML)
        webbrowser.open(OUTPUT_HTML)
    else:
        print("❌ 未获取到任何数据，请检查网络。")

if __name__ == '__main__':
    main()