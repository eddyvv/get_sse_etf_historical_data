"""
上交所宽基 ETF 规模监控 - 本地 Python 版
依赖安装：pip install requests plotly openpyxl

运行方式：python sse_etf_dashboard.py
输出文件：sse_final_dashboard.html  （交互图表，自动在浏览器中打开）
         sse_etf_data.xlsx          （历史数据，含「透视表」和「明细表」两个 Sheet）
"""

import requests
import json
import time
import webbrowser
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── 1. 配置区 ──────────────────────────────────────────────────────────────────

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

TARGET_DAYS   = 1500  # 2020至今约1500个交易日
MAX_LOOK_BACK = 2500  # 自然日缓冲（含节假日）
OUTPUT_HTML   = 'sse_final_dashboard.html'
OUTPUT_EXCEL  = 'sse_etf_data.xlsx'

HEADERS = {
    'User-Agent':  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer':     'https://www.sse.com.cn/',
    'Accept':      '*/*',
}

# ── 2. 数据抓取 ────────────────────────────────────────────────────────────────

def fetch_day(date_str):
    ts = int(time.time() * 1000)
    url = (
        'https://query.sse.com.cn/commonQuery.do'
        f'?isPagination=true'
        f'&pageHelp.pageSize=1000'
        f'&sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L'
        f'&STAT_DATE={date_str}'
        f'&_{ts}'
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10, proxies={'http': None, 'https': None})
        resp.raise_for_status()
        data  = resp.json()
        items = data.get('pageHelp', {}).get('data', [])
        return items if items else None
    except Exception as e:
        print(f'  [警告] {date_str} 请求异常: {e}')
        return None


def collect_trading_days():
    results = []
    today   = datetime.today()
    cutoff  = datetime(2020, 1, 1)   # 不抓 2020 年以前的数据

    for offset in range(MAX_LOOK_BACK):
        if len(results) >= TARGET_DAYS:
            break

        d = today - timedelta(days=offset)
        if d < cutoff:                # 到达截止日期则停止
            print(f'已到达截止日期 2020-01-01，停止采集。')
            break

        date_str = d.strftime('%Y-%m-%d')
        print(f'正在同步 {date_str}... (已获取 {len(results)}/{TARGET_DAYS} 交易日)', end='  ')
        items = fetch_day(date_str)
        if items:
            results.append({'date': date_str, 'items': items})
            print('✓')
        else:
            print('—（非交易日或无数据）')
        time.sleep(0.4)
    return results


# ── 3. 数据整理 ────────────────────────────────────────────────────────────────

def parse_val(v):
    if v is None:
        return None
    try:
        return float(str(v).replace(',', ''))
    except ValueError:
        return None


def sniff_keys(sample):
    keys     = list(sample.keys())
    code_key = next(
        (k for k in keys if str(sample[k]).strip()[:2] in ('51', '56', '58')),
        'SEC_CODE'
    )
    num_keys = [
        k for k in keys
        if k != code_key
        and 'DATE' not in k.upper()
        and parse_val(sample[k]) is not None
    ]
    val_key = (
        next((k for k in num_keys if any(kw in k.upper() for kw in ('VOL', 'FE', 'SHARE', '份额', '总量'))), None)
        or next((k for k in num_keys if any(kw in k.upper() for kw in ('VAL', 'SZ', '市值'))), None)
        or (num_keys[0] if num_keys else None)
    )
    return code_key, val_key


def build_plot_data(results):
    if not results:
        return []
    code_key, val_key = sniff_keys(results[0]['items'][0])
    print(f'\n字段嗅探结果 → 代码字段: {code_key}  |  数值字段: {val_key}\n')

    plot_data = []
    for code, name in ETF_MAP.items():
        dates, values = [], []
        for day in results:
            item = next(
                (i for i in day['items'] if str(i.get(code_key, '')).strip() == code),
                None
            )
            dates.append(day['date'])
            values.append(parse_val(item[val_key]) if item else None)

        pairs  = sorted(zip(dates, values), key=lambda x: x[0])
        dates  = [p[0] for p in pairs]
        values = [p[1] for p in pairs]

        plot_data.append({
            'x': dates, 'y': values,
            'name': f'{name}({code})',
            'mode': 'lines+markers',
            'line': {'width': 2.5},
            'marker': {'size': 6},
            'connectgaps': False,
        })
    return plot_data


# ── 4. Excel 导出 ──────────────────────────────────────────────────────────────

HDR_FILL  = PatternFill('solid', start_color='1F4E79')
SUB_FILL  = PatternFill('solid', start_color='2E75B6')
ALT_FILL  = PatternFill('solid', start_color='EBF3FB')
HDR_FONT  = Font(name='Arial', bold=True, color='FFFFFF', size=13)
SUB_FONT  = Font(name='Arial', bold=True, color='FFFFFF', size=10)
BODY_FONT = Font(name='Arial', size=10)
CENTER    = Alignment(horizontal='center', vertical='center')
LEFT_ALGN = Alignment(horizontal='left',   vertical='center')
NUM_FMT   = '#,##0.00'
THIN      = Side(style='thin', color='BDD7EE')
BORDER    = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)


def sc(cell, font=None, fill=None, alignment=None, number_format=None, border=None):
    if font:          cell.font          = font
    if fill:          cell.fill          = fill
    if alignment:     cell.alignment     = alignment
    if number_format: cell.number_format = number_format
    if border:        cell.border        = border


def generate_excel(plot_data, output_path):
    """
    Sheet 1 透视表：行=日期，列=各ETF
    Sheet 2 明细表：ETF名称 | 代码 | 日期 | 规模
    """
    wb  = Workbook()
    all_dates = sorted({d for trace in plot_data for d in trace['x']})
    etf_names = [t['name'] for t in plot_data]

    # ── Sheet 1：透视表 ────────────────────────────────────────────────────────
    ws1       = wb.active
    ws1.title = '透视表（日期×ETF）'

    ws1.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(etf_names) + 1)
    sc(ws1.cell(row=1, column=1, value='上交所宽基 ETF 规模历史数据（单位：万份）'),
       font=HDR_FONT, fill=HDR_FILL, alignment=CENTER)
    ws1.row_dimensions[1].height = 30

    sc(ws1.cell(row=2, column=1, value='统计日期'),
       font=SUB_FONT, fill=SUB_FILL, alignment=CENTER, border=BORDER)
    for ci, name in enumerate(etf_names, start=2):
        sc(ws1.cell(row=2, column=ci, value=name),
           font=SUB_FONT, fill=SUB_FILL, alignment=CENTER, border=BORDER)
    ws1.row_dimensions[2].height = 22

    date_to_row = {d: r for r, d in enumerate(all_dates, start=3)}
    for date in all_dates:
        r    = date_to_row[date]
        fill = ALT_FILL if r % 2 == 0 else None
        sc(ws1.cell(row=r, column=1, value=date),
           font=BODY_FONT, fill=fill, alignment=CENTER, border=BORDER)

    for ci, trace in enumerate(plot_data, start=2):
        for date, val in zip(trace['x'], trace['y']):
            r    = date_to_row[date]
            fill = ALT_FILL if r % 2 == 0 else None
            sc(ws1.cell(row=r, column=ci, value=val),
               font=BODY_FONT, fill=fill, alignment=CENTER,
               number_format=NUM_FMT, border=BORDER)

    ws1.column_dimensions['A'].width = 16
    for ci in range(2, len(etf_names) + 2):
        ws1.column_dimensions[get_column_letter(ci)].width = 26
    ws1.freeze_panes = 'A3'

    # ── Sheet 2：明细表 ────────────────────────────────────────────────────────
    ws2       = wb.create_sheet('明细表')
    headers   = ['ETF名称', '代码', '统计日期', '规模（万份）']
    col_widths = [28, 12, 16, 18]

    ws2.merge_cells(start_row=1, start_column=1, end_row=1, end_column=4)
    sc(ws2.cell(row=1, column=1, value='上交所宽基 ETF 规模历史明细'),
       font=HDR_FONT, fill=HDR_FILL, alignment=CENTER)
    ws2.row_dimensions[1].height = 30

    for ci, h in enumerate(headers, start=1):
        sc(ws2.cell(row=2, column=ci, value=h),
           font=SUB_FONT, fill=SUB_FILL, alignment=CENTER, border=BORDER)
    ws2.row_dimensions[2].height = 22

    row = 3
    for trace in plot_data:
        etf_name = trace['name'].split('(')[0]
        etf_code = trace['name'].split('(')[1].rstrip(')')
        for date, val in zip(trace['x'], trace['y']):
            fill = ALT_FILL if row % 2 == 0 else None
            for ci, v in enumerate([etf_name, etf_code, date, val], start=1):
                align = LEFT_ALGN if ci == 1 else CENTER
                fmt   = NUM_FMT  if ci == 4 else None
                sc(ws2.cell(row=row, column=ci, value=v),
                   font=BODY_FONT, fill=fill, alignment=align,
                   number_format=fmt, border=BORDER)
            row += 1

    for ci, w in enumerate(col_widths, start=1):
        ws2.column_dimensions[get_column_letter(ci)].width = w
    ws2.freeze_panes = 'A3'

    wb.save(output_path)
    print(f'✅ Excel 文件已生成：{output_path}  （共 {row - 3} 行明细）')


# ── 5. HTML 生成 ───────────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>上交所宽基 ETF 规模监控</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    body {
      background-color: #f5f7fa;
      color: #333;
      font-family: 'PingFang SC', 'Segoe UI', sans-serif;
      margin: 0;
      padding: 20px;
    }
    h2 {
      text-align: center;
      color: #2c3e50;
      font-weight: 500;
      margin-bottom: 20px;
    }
    .tabs-container {
      display: flex;
      flex-wrap: wrap;
      justify-content: center;
      gap: 10px;
      margin-bottom: 20px;
      width: 95%;
      margin-left: auto;
      margin-right: auto;
    }
    .etf-block {
      padding: 10px 18px;
      background-color: #fff;
      border: 1px solid #dcdfe6;
      border-radius: 8px;
      cursor: pointer;
      font-size: 14px;
      color: #606266;
      transition: all 0.2s ease;
      box-shadow: 0 2px 4px rgba(0,0,0,0.02);
    }
    .etf-block:hover  { border-color: #409EFF; color: #409EFF; }
    .etf-block.active {
      background-color: #409EFF;
      color: #fff;
      border-color: #409EFF;
      font-weight: bold;
      box-shadow: 0 4px 8px rgba(64,158,255,0.3);
    }
    .chart-container {
      width: 95%;
      height: 650px;
      margin: 0 auto;
      background-color: #fff;
      padding: 20px;
      border-radius: 12px;
      box-shadow: 0 4px 16px rgba(0,0,0,0.05);
    }
  </style>
</head>
<body>
  <h2>上交所宽基 ETF 规模监控看板（2020年至今 / 单位：万份）</h2>
  <div class="tabs-container" id="tabs"></div>
  <div class="chart-container" id="chart"></div>

  <script>
    var rawData = PLOT_DATA_JSON;

    var layout = {
      paper_bgcolor: '#ffffff',
      plot_bgcolor:  '#ffffff',
      title:  { text: '总体汇总视图', font: {size: 18} },
      xaxis:  {
        title: '统计日期（YYYY-MM-DD）',
        type:  'category',
        tickmode: 'linear',
        dtick:    30,
        tickangle: -45,
        gridcolor: '#f0f0f0'
      },
      yaxis:  { title: '规模（万份）', gridcolor: '#f0f0f0' },
      hovermode: 'x unified',
      legend: { orientation: 'v', x: 1.02, y: 1 },
      margin: { t: 60, r: 150, b: 80, l: 60 }
    };

    Plotly.newPlot('chart', rawData, layout);

    var tabsDiv = document.getElementById('tabs');

    var btnAll = document.createElement('div');
    btnAll.className   = 'etf-block active';
    btnAll.textContent = '📊 总体汇总';
    btnAll.onclick = function() {
      document.querySelectorAll('.etf-block').forEach(b => b.classList.remove('active'));
      this.classList.add('active');
      Plotly.restyle('chart', {visible: true});
      Plotly.relayout('chart', {title: '总体汇总视图'});
    };
    tabsDiv.appendChild(btnAll);

    rawData.forEach(function(trace, index) {
      var btn = document.createElement('div');
      btn.className   = 'etf-block';
      btn.textContent = trace.name.split('(')[0];
      btn.onclick = function() {
        document.querySelectorAll('.etf-block').forEach(b => b.classList.remove('active'));
        this.classList.add('active');
        var mask = rawData.map(function(_, i) { return i === index; });
        Plotly.update('chart', {visible: mask}, {title: trace.name + ' 规模走势'});
      };
      tabsDiv.appendChild(btn);
    });
  </script>
</body>
</html>"""


def generate_html(plot_data, output_path):
    html = HTML_TEMPLATE.replace('PLOT_DATA_JSON', json.dumps(plot_data, ensure_ascii=False))
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'✅ HTML 文件已生成：{output_path}')


# ── 6. 主流程 ──────────────────────────────────────────────────────────────────

def main():
    print('=' * 60)
    print('  上交所宽基 ETF 规模监控 - 数据采集中...')
    print('=' * 60)

    results = collect_trading_days()
    if not results:
        print('\n❌ 未获取到任何数据，请检查网络或稍后重试。')
        return

    print(f'\n共获取 {len(results)} 个有效交易日数据。\n')
    plot_data = build_plot_data(results)

    generate_html(plot_data, OUTPUT_HTML)
    generate_excel(plot_data, OUTPUT_EXCEL)

    webbrowser.open(OUTPUT_HTML)
    print('🌐 已在浏览器中打开 HTML 看板，若未自动打开请手动双击 HTML 文件。')


if __name__ == '__main__':
    main()