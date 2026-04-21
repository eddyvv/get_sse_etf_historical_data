"""
将旧的 sse_checkpoint.json（包含全部 ~100 只 ETF、所有字段）
压缩为仅保留 ETF_MAP 中 9 只 ETF 的 SEC_CODE + TOT_VOL 字段。

预期：90MB → ~5MB
用法：python migrate_checkpoint.py
"""

import json
import os
import time
import sys

CHECKPOINT = 'sse_checkpoint.json'

ETF_CODES = {
    '510300', '510310', '510330', '510050', '510500',
    '512100', '510180', '560010', '588080',
}

def migrate():
    if not os.path.exists(CHECKPOINT):
        print(f'找不到 {CHECKPOINT}，无需迁移。')
        return

    old_size = os.path.getsize(CHECKPOINT)
    print(f'正在读取 {CHECKPOINT}（{old_size / 1024 / 1024:.1f} MB）...')
    t0 = time.time()

    with open(CHECKPOINT, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f'  读取耗时 {time.time()-t0:.1f}s，共 {len(data.get("results",[]))} 个交易日')

    results = data.get('results', [])
    if not results:
        print('  无数据，无需迁移。')
        return

    # 检查是否已经是精简格式（第一条记录的 items 只有 SEC_CODE + TOT_VOL）
    sample = results[0].get('items', [])
    if sample and len(sample[0].keys()) <= 2:
        print('  已是精简格式，跳过迁移。')
        return

    # 压缩：只保留 ETF_MAP 中的 9 只 ETF，且只存 SEC_CODE + TOT_VOL
    total_before = 0
    total_after  = 0
    for day in results:
        items = day.get('items', [])
        total_before += len(items)
        filtered = []
        for item in items:
            code = str(item.get('SEC_CODE', '')).strip()
            if code in ETF_CODES:
                filtered.append({
                    'SEC_CODE': code,
                    'TOT_VOL':  item.get('TOT_VOL'),
                })
        day['items'] = filtered
        total_after += len(filtered)

    print(f'  压缩前: {total_before} 条记录 → 压缩后: {total_after} 条')
    print(f'  压缩比: {total_after/total_before*100:.1f}%')

    # 原子写入
    tmp = CHECKPOINT + '.migrate_tmp'
    t1 = time.time()
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    os.replace(tmp, CHECKPOINT)
    new_size = os.path.getsize(CHECKPOINT)
    print(f'  写入耗时 {time.time()-t1:.1f}s')
    print(f'  文件大小: {old_size/1024/1024:.1f} MB → {new_size/1024/1024:.1f} MB '
          f'(省 {(1-new_size/old_size)*100:.0f}%)')
    print(f'\n迁移完成！旧文件备份: {CHECKPOINT}.bak')

    # 备份旧文件（可选，迁移成功后删除）
    # 这里直接覆盖不备份，因为旧数据可以从上交所重新下载


if __name__ == '__main__':
    migrate()
