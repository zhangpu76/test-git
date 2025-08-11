import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 设置中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题


# 1. 加载数据
def load_data(file_path="customer_transactions.csv"):
    """加载客户交易数据"""
    df = pd.read_csv(file_path)
    # 转换消费时间为datetime类型
    df['consume_time'] = pd.to_datetime(df['consume_time'])
    print(f"数据加载完成，共{len(df)}条记录，包含{df['customer_id'].nunique()}个客户")
    return df


# 2. 计算RFM指标
def calculate_rfm(df):
    """计算RFM指标：最近消费时间(R)、消费频率(F)、消费金额(M)"""
    # 分析日期设为数据中最新消费时间的后一天
    analysis_date = df['consume_time'].max() + pd.Timedelta(days=1)

    # 按客户ID聚合计算RFM
    rfm = df.groupby('customer_id').agg({
        'consume_time': lambda x: (analysis_date - x.max()).days,  # R：最近消费间隔（天）
        'customer_id': lambda x: x.count(),  # F：消费频率
        'amount': lambda x: x.sum()  # M：消费总金额
    }).rename(columns={
        'consume_time': 'R',
        'customer_id': 'F',
        'amount': 'M'
    })

    print("RFM指标计算完成：")
    print(rfm.describe())
    return rfm


# 3. RFM评分与客户分层
def rfm_segmentation(rfm):
    """对RFM指标评分并进行客户分层"""
    # 按指标值大小评分（1-5分，R越小得分越高，F和M越大得分越高）
    rfm['R_score'] = pd.qcut(rfm['R'], 5, labels=[5, 4, 3, 2, 1], duplicates='drop')
    rfm['F_score'] = pd.qcut(rfm['F'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
    rfm['M_score'] = pd.qcut(rfm['M'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')

    # 转换为数值型以便计算总分
    rfm[['R_score', 'F_score', 'M_score']] = rfm[['R_score', 'F_score', 'M_score']].astype(int)
    rfm['RFM_total'] = rfm['R_score'] + rfm['F_score'] + rfm['M_score']

    # 客户分层规则（经典RFM模型分类）
    def segment_customer(row):
        if row['R_score'] >= 4 and row['F_score'] >= 4 and row['M_score'] >= 4:
            return '高价值客户'
        elif row['R_score'] >= 3 and row['F_score'] >= 3 and row['M_score'] >= 3:
            return '潜力客户'
        elif row['R_score'] >= 4 and row['F_score'] <= 2 and row['M_score'] <= 2:
            return '新客户'
        elif row['R_score'] <= 2 and row['F_score'] >= 3 and row['M_score'] >= 3:
            return '流失客户'
        else:
            return '一般客户'

    rfm['customer_segment'] = rfm.apply(segment_customer, axis=1)
    return rfm


# 4. 可视化分析
def visualize_rfm(rfm):
    """可视化RFM分析结果"""
    # 1. 客户分层数量分布
    plt.figure(figsize=(10, 6))
    segment_counts = rfm['customer_segment'].value_counts()
    sns.barplot(x=segment_counts.index, y=segment_counts.values)
    plt.title('客户分层数量分布')
    plt.xlabel('客户类型')
    plt.ylabel('客户数量')
    plt.xticks(rotation=30)
    for i, v in enumerate(segment_counts.values):
        plt.text(i, v + 1, str(v), ha='center')
    plt.tight_layout()
    plt.show()

    # 2. 各分层RFM均值对比
    plt.figure(figsize=(12, 8))
    segment_rfm = rfm.groupby('customer_segment')[['R', 'F', 'M']].mean().reset_index()
    segment_rfm = pd.melt(segment_rfm, id_vars='customer_segment', var_name='指标', value_name='均值')

    sns.barplot(x='customer_segment', y='均值', hue='指标', data=segment_rfm)
    plt.title('各客户分层RFM指标均值对比')
    plt.xlabel('客户类型')
    plt.ylabel('指标均值')
    plt.xticks(rotation=30)
    plt.legend(title='RFM指标')
    plt.tight_layout()
    plt.show()


# 主函数
def main():
    # 加载数据
    df = load_data()
    # 计算RFM
    rfm = calculate_rfm(df)
    # 客户分层
    rfm_segmented = rfm_segmentation(rfm)
    # 保存结果
    rfm_segmented.to_csv('rfm_customer_segmentation.csv')
    print("RFM分析结果已保存至rfm_customer_segmentation.csv")
    # 可视化
    visualize_rfm(rfm_segmented)


if __name__ == "__main__":
    main()