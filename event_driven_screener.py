#!/usr/bin/env python3
"""
事件驱动选股器 - 基于新闻舆情和公告事件
核心逻辑：产业趋势(订单/技术突破) + 业绩拐点 + 资金确认
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== 配置 ====================

# 事件关键词（产业趋势信号）
EVENT_KEYWORDS = {
    'order': ['订单', '合同', '中标', '签订', '大单', '采购', '供货', '合作'],
    'tech_breakthrough': ['量产', '突破', '国产化', '替代进口', '技术领先', '专利'],
    'capacity_expansion': ['扩产', '投产', '新建', '产能', '产线', '工厂'],
    'price_increase': ['涨价', '供不应求', '库存下降', '紧缺', '价格上调'],
    'policy_support': ['国家战略', '专项资金', '扶持政策', '产业规划'],
}

# 筛选条件
FILTERS = {
    'revenue_growth_min': 30,      # 营收增速 > 30%（拐点）
    'gross_margin_up': True,        # 毛利率提升
    'news_count_min': 3,            # 近30天相关新闻 >= 3条
    'main_inflow_positive': True,   # 主力资金流入为正
    'score_min': 60,                # 综合得分 >= 60分
}

# 限速配置
RATE_LIMIT = 3

def rate_limit():
    time.sleep(RATE_LIMIT)

# ==================== 数据获取函数 ====================

def get_all_stocks():
    """获取全市场股票列表"""
    try:
        df = ak.stock_zh_a_spot_em()
        return df[['代码', '名称']]
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return None

def get_financial_report():
    """获取业绩报表（东财，限速）"""
    try:
        rate_limit()
        df = ak.stock_yjbb_em()
        return df
    except Exception as e:
        print(f"获取业绩报表失败: {e}")
        return None

def get_stock_news(symbol):
    """获取个股新闻（东财，限速）"""
    try:
        rate_limit()
        df = ak.stock_news_em(symbol=symbol)
        return df
    except Exception as e:
        return None

def get_stock_fund_flow():
    """获取个股资金流（同花顺）"""
    try:
        df = ak.stock_fund_flow_individual(symbol='即时')
        return df
    except Exception as e:
        print(f"获取资金流失败: {e}")
        return None

# ==================== 事件分析函数 ====================

def fetch_full_article(url):
    """用Jina Reader获取完整新闻内容"""
    try:
        import urllib.request
        jina_url = f"https://r.jina.ai/{url}"
        with urllib.request.urlopen(jina_url, timeout=10) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        return None

def analyze_news_with_llm(title, content, article_text):
    """用大模型分析新闻是否有产业事件"""
    # 简化版：用关键词匹配+简单规则
    # 实际应该用大模型API，这里先做基础版
    
    events = {
        'order': 0,
        'tech_breakthrough': 0,
        'capacity_expansion': 0,
        'price_increase': 0,
        'policy_support': 0,
    }
    
    # 合并所有文本
    full_text = title + ' ' + content
    if article_text:
        full_text += ' ' + article_text[:1000]  # 只取前1000字
    
    # 关键词匹配
    for event_type, keywords in EVENT_KEYWORDS.items():
        for keyword in keywords:
            if keyword in full_text:
                events[event_type] += 1
                break
    
    return events

def analyze_news_events(df_news, stock_name):
    """分析新闻中的事件信号（增强版）"""
    if df_news is None or len(df_news) == 0:
        return {}
    
    events = {
        'order': 0,
        'tech_breakthrough': 0,
        'capacity_expansion': 0,
        'price_increase': 0,
        'policy_support': 0,
    }
    
    # 只分析近30天的新闻，最多分析前5条
    cutoff_date = datetime.now() - timedelta(days=30)
    analyzed = 0
    
    for _, row in df_news.iterrows():
        if analyzed >= 5:  # 最多分析5条
            break
            
        # 检查日期
        try:
            news_date = pd.to_datetime(row['发布时间'])
            if news_date < cutoff_date:
                continue
        except:
            continue
        
        title = str(row.get('新闻标题', ''))
        content = str(row.get('新闻内容', ''))
        news_link = row.get('新闻链接', '')
        
        # 获取完整文章内容（如果有链接）
        article_text = None
        if news_link and 'eastmoney.com' in news_link:
            article_text = fetch_full_article(news_link)
            if article_text:
                time.sleep(1)  # 限速
        
        # 分析事件
        news_events = analyze_news_with_llm(title, content, article_text)
        
        # 累加事件
        for event_type in events:
            events[event_type] += news_events[event_type]
        
        analyzed += 1
    
    return events

# ==================== 主程序 ====================

def main():
    print("=" * 60)
    print(f"事件驱动选股器 [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    print("=" * 60)
    print()
    
    start_time = time.time()
    
    # 步骤1：获取业绩数据
    print("【步骤1】获取业绩数据...")
    df_financial = get_financial_report()
    if df_financial is None:
        print("业绩数据获取失败，退出")
        return
    
    # 转换数据类型
    df_financial['营收增速'] = pd.to_numeric(df_financial['营业总收入-同比增长'], errors='coerce')
    df_financial['净利润增速'] = pd.to_numeric(df_financial['净利润-同比增长'], errors='coerce')
    
    # 筛选营收拐点
    df_growth = df_financial[df_financial['营收增速'] > FILTERS['revenue_growth_min']].copy()
    print(f"  营收增速>{FILTERS['revenue_growth_min']}%: {len(df_growth)}只")
    print()
    
    # 步骤2：获取资金流数据
    print("【步骤2】获取资金流数据...")
    df_fund = get_stock_fund_flow()
    if df_fund is None:
        print("资金流获取失败，退出")
        return
    
    # 转换净额
    def parse_amount(x):
        if isinstance(x, str):
            x = x.replace(',', '')
            if '亿' in x:
                return float(x.replace('亿', '')) * 10000
            elif '万' in x:
                return float(x.replace('万', ''))
        return float(x)
    
    df_fund['净额_万'] = df_fund['净额'].apply(parse_amount)
    print(f"  获取到 {len(df_fund)} 只股票资金流")
    print()
    
    # 步骤3：筛选资金流入的股票
    print("【步骤3】筛选资金流入的股票...")
    if FILTERS['main_inflow_positive']:
        df_fund = df_fund[df_fund['净额_万'] > 0]
    print(f"  资金流入: {len(df_fund)}只")
    print()
    
    # 步骤4：新闻事件分析（只分析前50只）
    print("【步骤4】新闻事件分析...")
    print("  分析资金流入前50只股票的新闻...")
    
    results = []
    for i, (_, stock) in enumerate(df_fund.head(50).iterrows()):
        code = str(stock['股票代码']).zfill(6)
        name = stock['股票简称']
        
        # 检查是否在业绩拐点列表
        fin_match = df_growth[df_growth['股票代码'] == code]
        if len(fin_match) == 0:
            continue
        
        fin = fin_match.iloc[0]
        revenue_growth = fin['营收增速']
        profit_growth = fin['净利润增速']
        
        # 获取新闻
        df_news = get_stock_news(code)
        if df_news is None:
            continue
        
        # 分析事件
        events = analyze_news_events(df_news, name)
        total_events = sum(events.values())
        
        if total_events < FILTERS['news_count_min']:
            continue
        
        # 计算得分
        score = 0
        
        # 营收增速 20分
        if revenue_growth > 100:
            score += 20
        elif revenue_growth > 50:
            score += 15
        else:
            score += 10
        
        # 事件信号 40分
        if events['order'] > 0:
            score += 15
        if events['tech_breakthrough'] > 0:
            score += 15
        if events['capacity_expansion'] > 0:
            score += 10
        
        # 资金流向 20分
        inflow = float(stock['净额_万'])
        if inflow > 50000:
            score += 20
        elif inflow > 20000:
            score += 15
        else:
            score += 10
        
        if score >= FILTERS['score_min']:
            results.append({
                '代码': code,
                '名称': name,
                '最新价': stock['最新价'],
                '涨跌幅': stock['涨跌幅'],
                '净流入': inflow,
                '营收增速': revenue_growth,
                '净利润增速': profit_growth,
                '订单事件': events['order'],
                '技术突破': events['tech_breakthrough'],
                '扩产事件': events['capacity_expansion'],
                '涨价事件': events['price_increase'],
                '政策支持': events['policy_support'],
                '得分': score,
            })
        
        if (i + 1) % 10 == 0:
            print(f"  已分析 {i+1}/50 只...")
    
    print(f"\n  事件分析完成，{len(results)}只符合条件")
    print()
    
    # 排序输出
    results.sort(key=lambda x: x['得分'], reverse=True)
    
    print("=" * 60)
    print("🎯 事件驱动精选 TOP 20")
    print("=" * 60)
    
    for i, r in enumerate(results[:20], 1):
        print(f"\n{i}. {r['名称']} ({r['代码']})  得分:{r['得分']}")
        print(f"   现价:{r['最新价']} 涨跌:{r['涨跌幅']}")
        print(f"   营收增速:{r['营收增速']:.1f}%  净利润增速:{r['净利润增速']:.1f}%")
        print(f"   净流入:{r['净流入']/10000:.1f}万")
        print(f"   📰 事件信号:")
        if r['订单事件'] > 0:
            print(f"      - 订单/合同: {r['订单事件']}条")
        if r['技术突破'] > 0:
            print(f"      - 技术突破: {r['技术突破']}条")
        if r['扩产事件'] > 0:
            print(f"      - 产能扩张: {r['扩产事件']}条")
        if r['涨价事件'] > 0:
            print(f"      - 涨价/供不应求: {r['涨价事件']}条")
        if r['政策支持'] > 0:
            print(f"      - 政策支持: {r['政策支持']}条")
    
    # 保存结果
    output_file = f"/tmp/event_driven_stocks_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'top_stocks': results[:20]
        }, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ 结果已保存: {output_file}")
    print(f"共筛选出 {len(results)} 只符合条件的股票")
    print(f"总用时: {time.time() - start_time:.1f}秒")

if __name__ == "__main__":
    main()