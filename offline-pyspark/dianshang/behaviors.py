import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score

# 设置中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
sns.set_style("whitegrid")


# 1. 数据加载与预处理（重点修复时间字段处理）
def load_and_preprocess():
    """读取behaviors.csv并预处理，确保时间字段正确解析"""
    # 读取CSV文件
    df = pd.read_csv(
        'behaviors.csv',
        sep=',',
        header=0,
        low_memory=False
    )

    # 验证字段
    expected_columns = ['behavior_id', 'user_id', 'shop_id', 'product_id',
                        'page_type', 'behavior_type', 'behavior_time',
                        'stay_duration', 'platform', 'referrer', 'detail']

    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        print(f"警告：数据中缺少预期字段 {missing_cols}")

    # 重点修复：时间字段解析
    if 'behavior_time' in df.columns:
        # 尝试多种时间格式解析
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y%m%d %H:%M:%S']:
            try:
                df['behavior_time'] = pd.to_datetime(df['behavior_time'], format=fmt)
                print(f"时间字段解析成功，使用格式: {fmt}")
                break
            except ValueError:
                continue
        else:
            # 如果所有格式都失败，使用自动解析
            df['behavior_time'] = pd.to_datetime(df['behavior_time'], errors='coerce')
            invalid_count = df['behavior_time'].isna().sum()
            print(f"警告：无法完全解析时间字段，{invalid_count}条记录时间无效")

    # 数据类型转换
    df['stay_duration'] = pd.to_numeric(df['stay_duration'], errors='coerce')

    # 仅在时间字段有效时提取日期特征
    if 'behavior_time' in df.columns and pd.api.types.is_datetime64_any_dtype(df['behavior_time']):
        df['date'] = df['behavior_time'].dt.date
        df['hour'] = df['behavior_time'].dt.hour
        df['weekday'] = df['behavior_time'].dt.weekday
    else:
        df['date'] = None
        df['hour'] = None
        df['weekday'] = None
        print("警告：时间字段无效，无法提取日期特征")

    # 缺失值处理
    df['product_id'] = df['product_id'].fillna('N/A')
    df['stay_duration'] = df['stay_duration'].fillna(
        df['stay_duration'].median() if 'stay_duration' in df.columns else 0)
    df['referrer'] = df['referrer'].fillna('未知来源')
    df['detail'] = df['detail'].fillna('无详情')

    print(f"数据加载完成，共{len(df)}条记录，{len(df.columns)}个字段")
    return df


# 2. 分箱工具函数
class BinningTools:
    @staticmethod
    def equal_width_bin(data, col, n_bins, labels=None):
        if col not in data.columns:
            print(f"警告：列{col}不存在，无法分箱")
            return pd.Series([None] * len(data), index=data.index)
        binned = pd.cut(data[col], bins=n_bins, labels=labels)
        return binned

    @staticmethod
    def equal_freq_bin(data, col, n_bins, labels=None):
        if col not in data.columns:
            print(f"警告：列{col}不存在，无法分箱")
            return pd.Series([None] * len(data), index=data.index)
        binned = pd.qcut(data[col], q=n_bins, labels=labels, duplicates='drop')
        return binned

    @staticmethod
    def custom_bin(data, col, bins, labels):
        if col not in data.columns:
            print(f"警告：列{col}不存在，无法分箱")
            return pd.Series([None] * len(data), index=data.index)
        binned = pd.cut(data[col], bins=bins, labels=labels, include_lowest=True)
        return binned


# 3. 标签模型开发
class TagGenerator:
    def __init__(self, df):
        self.df = df
        self.user_tags = None
        self.page_tags = None
        self.channel_tags = None
        self.binner = BinningTools()

    # 3.1 用户标签（修复时间字段处理）
    def generate_user_tags(self):
        # 检查必要字段
        required_cols = ['user_id', 'behavior_id', 'behavior_type']
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            print(f"警告：缺少必要字段{missing_cols}，用户标签生成可能不完整")
            return None

        # 基础指标计算
        agg_params = {
            '总行为次数': ('behavior_id', 'count'),
            '总访问会话数': ('behavior_id', 'nunique'),
            '平均停留时长': ('stay_duration', 'mean') if 'stay_duration' in self.df.columns else ('user_id', 'count'),
            '常用平台': (
            'platform', lambda x: x.mode()[0] if not x.mode().empty else '未知') if 'platform' in self.df.columns else (
            'user_id', lambda x: '未知'),
            '购买次数': ('behavior_type', lambda x: (x == '购买').sum()),
            '加购次数': ('behavior_type', lambda x: (x == '加购').sum()),
            '跳出次数': ('behavior_type', lambda x: (x == '跳出').sum()),
            '浏览次数': ('behavior_type', lambda x: (x == '浏览').sum())
        }

        # 仅在时间字段有效时添加时间相关指标
        if 'behavior_time' in self.df.columns and pd.api.types.is_datetime64_any_dtype(self.df['behavior_time']):
            agg_params['首次行为时间'] = ('behavior_time', 'min')
            agg_params['末次行为时间'] = ('behavior_time', 'max')

        user_stats = self.df.groupby('user_id').agg(**agg_params).reset_index()

        # 衍生指标计算（增加时间类型校验）
        user_stats['是否购买过'] = (user_stats['购买次数'] > 0).astype(int)

        # 修复核心错误：增加时间字段有效性检查
        if '首次行为时间' in user_stats.columns and '末次行为时间' in user_stats.columns:
            # 确保时间字段是datetime类型
            if pd.api.types.is_datetime64_any_dtype(user_stats['首次行为时间']) and \
                    pd.api.types.is_datetime64_any_dtype(user_stats['末次行为时间']):

                # 计算行为总天数（使用.dt访问器前确保类型正确）
                user_stats['行为总天数'] = (user_stats['末次行为时间'] - user_stats['首次行为时间']).dt.days
                user_stats['行为总天数'] = user_stats['行为总天数'].replace([0, -1], 1)  # 处理同一天或异常值
                user_stats['日均行为次数'] = user_stats['总行为次数'] / user_stats['行为总天数']
            else:
                print("警告：首次/末次行为时间不是datetime类型，无法计算行为天数")
                user_stats['日均行为次数'] = user_stats['总行为次数']  # 降级处理
        else:
            print("警告：缺少时间字段，无法计算行为天数")
            user_stats['日均行为次数'] = user_stats['总行为次数']  # 降级处理

        # 连续变量分箱
        user_stats['停留时长分箱'] = self.binner.custom_bin(
            user_stats, '平均停留时长',
            bins=[0, 10, 30, 60, np.inf],
            labels=['极短停留', '短停留', '中停留', '长停留']
        )

        user_stats['活跃度分箱'] = self.binner.equal_freq_bin(
            user_stats, '日均行为次数',
            n_bins=3,
            labels=['低活跃', '中活跃', '高活跃']
        )

        user_stats['购买频率分箱'] = self.binner.custom_bin(
            user_stats, '购买次数',
            bins=[-1, 0, 1, 3, np.inf],
            labels=['未购买', '偶尔购买', '经常购买', '高频购买']
        )

        # 用户价值标签
        user_stats['用户价值标签'] = '普通用户'
        user_stats.loc[
            (user_stats['停留时长分箱'] == '长停留') &
            (user_stats['活跃度分箱'] == '高活跃') &
            (user_stats['购买频率分箱'].isin(['经常购买', '高频购买'])),
            '用户价值标签'
        ] = '高价值用户'

        user_stats.loc[
            (user_stats['停留时长分箱'].isin(['中停留', '长停留'])) &
            (user_stats['加购次数'] > 0) &
            (user_stats['是否购买过'] == 0),
            '用户价值标签'
        ] = '潜力用户'

        user_stats.loc[
            (user_stats['停留时长分箱'] == '极短停留') &
            (user_stats['活跃度分箱'] == '低活跃') &
            (user_stats['是否购买过'] == 0),
            '用户价值标签'
        ] = '流失风险用户'

        self.user_tags = user_stats
        print(f"\n用户标签生成完成，共{len(user_stats)}个用户")
        print("用户价值标签分布：")
        print(user_stats['用户价值标签'].value_counts())
        return user_stats

    # 3.2 页面标签
    def generate_page_tags(self):
        if 'page_type' not in self.df.columns:
            print("警告：缺少page_type列，无法生成页面标签")
            return None

        page_stats = self.df.groupby('page_type').agg(
            总访问量=('behavior_id', 'nunique'),
            平均停留时长=('stay_duration', 'mean') if 'stay_duration' in self.df.columns else ('page_type', 'count'),
            跳出次数=('behavior_type', lambda x: (x == '跳出').sum()) if 'behavior_type' in self.df.columns else 0,
            加购次数=('behavior_type', lambda x: (x == '加购').sum()) if 'behavior_type' in self.df.columns else 0,
            购买次数=('behavior_type', lambda x: (x == '购买').sum()) if 'behavior_type' in self.df.columns else 0
        ).reset_index()

        if '总访问量' in page_stats.columns and page_stats['总访问量'].sum() > 0:
            page_stats['跳出率'] = page_stats['跳出次数'] / page_stats['总访问量'] * 100
            page_stats['转化率'] = (page_stats['加购次数'] + page_stats['购买次数']) / page_stats['总访问量'] * 100

            page_stats['跳出率分箱'] = self.binner.custom_bin(
                page_stats, '跳出率',
                bins=[0, 30, 60, 100],
                labels=['低跳出率', '中跳出率', '高跳出率']
            )

            page_stats['转化率分箱'] = self.binner.custom_bin(
                page_stats, '转化率',
                bins=[0, 5, 15, 100],
                labels=['低转化', '中转化', '高转化']
            )

            page_stats['页面质量标签'] = '普通页面'
            page_stats.loc[page_stats['跳出率分箱'] == '高跳出率', '页面质量标签'] = '需优化页面'
            page_stats.loc[page_stats['转化率分箱'] == '高转化', '页面质量标签'] = '优质转化页面'

        self.page_tags = page_stats
        print(f"\n页面标签生成完成，共{len(page_stats)}种页面类型")
        return page_stats

    # 3.3 渠道标签
    def generate_channel_tags(self):
        if 'referrer' not in self.df.columns:
            print("警告：缺少referrer列，无法生成渠道标签")
            return None

        channel_stats = self.df.groupby('referrer').agg(
            总引流次数=('behavior_id', 'nunique'),
            带来用户数=('user_id', 'nunique'),
            总购买次数=('behavior_type', lambda x: (x == '购买').sum()) if 'behavior_type' in self.df.columns else 0
        ).reset_index()

        if '总引流次数' in channel_stats.columns and '带来用户数' in channel_stats.columns:
            channel_stats['人均行为次数'] = channel_stats['总引流次数'] / channel_stats['带来用户数'].replace(0, 1)
            channel_stats['购买转化率'] = channel_stats['总购买次数'] / channel_stats['总引流次数'].replace(0, 1) * 100

            channel_stats['引流能力分箱'] = self.binner.equal_freq_bin(
                channel_stats, '总引流次数',
                n_bins=3,
                labels=['弱引流', '中引流', '强引流']
            )

            channel_stats['渠道价值标签'] = '普通渠道'
            channel_stats.loc[
                (channel_stats['引流能力分箱'] == '强引流') &
                (channel_stats['购买转化率'] > 10),
                '渠道价值标签'
            ] = '核心渠道'

        self.channel_tags = channel_stats
        print(f"\n渠道标签生成完成，共{len(channel_stats)}个渠道")
        return channel_stats


# 4. 算法模型构建
class AlgorithmModels:
    def __init__(self, df, user_tags):
        self.df = df
        self.user_tags = user_tags
        self.features = None

    def prepare_features(self):
        if self.user_tags is None:
            print("警告：用户标签为空，无法准备特征")
            return None

        features = self.user_tags[
            ['user_id', '是否购买过', '总行为次数', '平均停留时长',
             '日均行为次数', '停留时长分箱', '活跃度分箱', '购买频率分箱']
        ]

        features = pd.get_dummies(
            features,
            columns=['停留时长分箱', '活跃度分箱', '购买频率分箱'],
            prefix=['停留', '活跃', '购买']
        )

        self.features = features
        print("\n特征工程完成，特征列表：")
        print(features.columns.tolist())
        return features

    def build_clustering_model(self):
        if self.features is None:
            print("警告：特征为空，无法构建聚类模型")
            return None

        print("\n===== 构建用户聚类模型 =====")
        X = self.features.drop('user_id', axis=1)
        X_scaled = StandardScaler().fit_transform(X)

        kmeans = KMeans(n_clusters=4, random_state=42)
        self.features['聚类结果'] = kmeans.fit_predict(X_scaled)

        plt.figure(figsize=(12, 8))
        if '日均行为次数' in self.features.columns and '平均停留时长' in self.features.columns:
            sns.scatterplot(
                data=self.features,
                x='日均行为次数',
                y='平均停留时长',
                hue='聚类结果',
                palette='viridis',
                s=100
            )
            plt.title('用户聚类结果（按日均行为次数和平均停留时长）')
            plt.show()

        return kmeans

    def build_purchase_prediction_model(self):
        if self.features is None:
            print("警告：特征为空，无法构建购买预测模型")
            return None

        print("\n===== 构建购买预测模型 =====")
        X = self.features.drop(['user_id', '是否购买过'], axis=1)
        y = self.features['是否购买过']

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42, stratify=y
        )

        model = LogisticRegression(class_weight='balanced', max_iter=1000)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        print(f"模型评估指标：")
        print(f"准确率：{accuracy_score(y_test, y_pred):.4f}")
        print(f"精确率：{precision_score(y_test, y_pred):.4f}")
        print(f"AUC：{roc_auc_score(y_test, y_prob):.4f}")

        return model


# 主函数
def main():
    df = load_and_preprocess()
    if df is None or len(df) == 0:
        print("数据加载失败，无法继续处理")
        return

    tag_generator = TagGenerator(df)
    user_tags = tag_generator.generate_user_tags()
    page_tags = tag_generator.generate_page_tags()
    channel_tags = tag_generator.generate_channel_tags()

    if user_tags is not None:
        user_tags.to_csv('user_tags_with_binning.csv', index=False)
        print("\n用户标签已保存为 user_tags_with_binning.csv")

    if user_tags is not None:
        algo_models = AlgorithmModels(df, user_tags)
        algo_models.prepare_features()
        algo_models.build_clustering_model()
        algo_models.build_purchase_prediction_model()


if __name__ == "__main__":
    main()
