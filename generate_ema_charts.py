#!/usr/bin/env python3
"""
生成EMA选股CSV中股票的K线图像
使用CryptoScanner/all.py的generate_chart_image函数风格
"""

import os
import sys
import glob
import io
import akshare as ak
import pandas as pd
import numpy as np
import mplfinance as mpf
import talib
from datetime import datetime, timedelta

# 添加CryptoScanner路径
sys.path.insert(0, '/root/CryptoScanner')

# 配置
EMA_CSV_PATH = "/root/history_AStock/EMA_Inclusive_Breakout_Strict_*.csv"
OUTPUT_DIR = "/root/history_AStock/ema_charts"
DAYS_OF_DATA = 120  # 获取120天数据

# 确保输出目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_latest_ema_csv():
    """获取最新的EMA CSV文件"""
    files = glob.glob(EMA_CSV_PATH)
    if not files:
        print("❌ 未找到EMA CSV文件")
        return None
    latest = max(files, key=os.path.getctime)
    print(f"📁 使用文件: {latest}")
    return latest

import time

# 本地历史数据目录
LOCAL_DATA_DIR = "/root/history_AStock/data_day"

def get_stock_kline(symbol, days=120):
    """从本地读取股票K线数据"""
    try:
        # 构建本地文件路径
        local_file = os.path.join(LOCAL_DATA_DIR, f"{symbol}.csv")
        
        if not os.path.exists(local_file):
            print(f"   ⚠️  本地数据不存在: {local_file}")
            return None
        
        # 读取本地CSV
        df = pd.read_csv(local_file)
        
        if df is None or len(df) < 60:
            print(f"   ⚠️  数据不足: {len(df)} 条")
            return None
        
        # 只取最近days天
        df = df.tail(days).copy()
        
        # 转换列名以匹配CryptoScanner格式
        df['time'] = pd.to_datetime(df['date']).astype('int64') // 10**6  # 转为毫秒时间戳
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        return df[['time', 'open', 'high', 'low', 'close', 'volume']]
    except Exception as e:
        print(f"❌ 读取{symbol}数据失败: {str(e)[:50]}")
        return None

def generate_chart_image(df, symbol, name, industry="", ema20_val=None, ema60_val=None):
    """
    生成K线图像（基于CryptoScanner/all.py风格）
    """
    try:
        if df is None or len(df) < 60:
            return None
        
        # 数据清洗
        plot_df = df.copy()
        plot_df['close'] = plot_df['close'].astype(float)
        plot_df['high'] = plot_df['high'].astype(float)
        plot_df['low'] = plot_df['low'].astype(float)
        plot_df['open'] = plot_df['open'].astype(float)
        plot_df['volume'] = plot_df['volume'].astype(float)
        
        # UTC -> 北京时间
        plot_df['time'] = pd.to_datetime(plot_df['time'], unit='ms')
        plot_df['time'] = plot_df['time'] + pd.Timedelta(hours=8)
        plot_df.set_index('time', inplace=True)
        
        # 计算EMA
        ema20 = talib.EMA(plot_df['close'].values, timeperiod=20)
        ema60 = talib.EMA(plot_df['close'].values, timeperiod=60)
        ema120 = talib.EMA(plot_df['close'].values, timeperiod=120)
        
        # 检查EMA数据有效性
        valid_ema20 = ema20[~np.isnan(ema20)]
        valid_ema60 = ema60[~np.isnan(ema60)]
        valid_ema120 = ema120[~np.isnan(ema120)]
        
        if len(valid_ema20) < 10 or len(valid_ema60) < 10:
            print(f"   ⚠️  EMA数据不足")
            return None
        
        # EMA颜色 (白、蓝、紫)
        apds = [
            mpf.make_addplot(ema20, color='white', width=1.2),
            mpf.make_addplot(ema60, color='#2196F3', width=1.2),
            mpf.make_addplot(ema120, color='#9C27B0', width=1.2),
        ]
        
        # 设置样式
        mc = mpf.make_marketcolors(
            up='#2ebd85', down='#f6465d',
            edge='inherit', wick='inherit',
            volume='in',
            inherit=True
        )
        
        s = mpf.make_mpf_style(
            base_mpf_style='nightclouds',
            marketcolors=mc,
            gridstyle=':',
            y_on_right=True,
            rc={
                'font.size': 12,
                'font.family': 'DejaVu Sans',
                'axes.unicode_minus': False
            }
        )
        
        # 生成文件名（加上行业）
        safe_name = name.replace('/', '-').replace(' ', '_')
        safe_industry = industry.replace('/', '-').replace(' ', '_')[:10]  # 行业名截取前10字符
        output_file = os.path.join(OUTPUT_DIR, f"{symbol}_{safe_name}_{safe_industry}.png")
        
        # 绘制并保存
        mpf.plot(
            plot_df,
            type='candle',
            style=s,
            addplot=apds,
            volume=True,
            title=f"\n{symbol} {name}\n行业: {industry} - EMA Breakout",
            figsize=(18, 10),
            datetime_format='%m-%d',
            tight_layout=True,
            savefig=dict(fname=output_file, dpi=150, bbox_inches='tight', transparent=False),
            block=False
        )
        
        print(f"✅ 已生成: {output_file}")
        return output_file
        
    except Exception as e:
        print(f"❌ 画图失败 {symbol}: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    print("=" * 60)
    print("EMA选股K线图生成器")
    print("=" * 60)
    print()
    
    # 获取最新CSV
    csv_file = get_latest_ema_csv()
    if not csv_file:
        return
    
    # 读取CSV
    try:
        df = pd.read_csv(csv_file)
        print(f"📊 共 {len(df)} 只股票需要生成图表")
        print()
    except Exception as e:
        print(f"❌ 读取CSV失败: {e}")
        return
    
    # 生成图表
    success_count = 0
    fail_count = 0
    
    for idx, row in df.iterrows():
        symbol = row['代码']
        name = row['名称']
        industry = row.get('行业', '')  # 获取行业信息
        
        print(f"[{idx+1}/{len(df)}] 处理 {symbol} {name} ({industry})...")
        
        # 获取K线数据
        kline_df = get_stock_kline(symbol, DAYS_OF_DATA)
        if kline_df is None:
            print(f"   ⚠️  跳过（无数据）")
            fail_count += 1
            continue
        
        # 生成图表（传入行业）
        result = generate_chart_image(kline_df, symbol, name, industry,
                                      row.get('EMA20'), row.get('EMA60'))
        if result:
            success_count += 1
        else:
            fail_count += 1
    
    print()
    print("=" * 60)
    print(f"✅ 完成: {success_count} 成功, {fail_count} 失败")
    print(f"📁 图表保存至: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
