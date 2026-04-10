# 获取上证交易所部分ETF历史数据

此仓库用于获取上证交易所部分[ETF](#ETF)的历史数据，时间从2020年1月1日至今。

## 环境

* Windows11

* Python 3.8.6

* claude

依赖包：

```bash
pip install requests plotly openpyxl
```

## 使用

```bash
python sse_etf_dashboard.py
```

## ETF

| 名称                  | 代码   |
| --------------------- | ------ |
| 华泰柏瑞沪深300ETF    | 510300 |
| 易方达沪深300ETF      | 510310 |
| 华夏沪深300ETF        | 510330 |
| 华夏上证50ETF         | 510050 |
| 南方中证500ETF        | 510500 |
| 南方中证1000ETF       | 512100 |
| 华安上证180ETF        | 510180 |
| 广发中证1000ETF       | 560010 |
| 易方达上证科创板50ETF | 588080 |
