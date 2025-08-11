import pandas as pd
import numpy as np
import re
import jieba
import jieba.analyse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from collections import Counter
import matplotlib.pyplot as plt

# 解决 Matplotlib 中文显示问题
plt.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体为 SimHei（支持中文）
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示为方块的问题

# 1. 加载数据
df = pd.read_csv("ecommerce_products.csv")
# 选择需要的字段（商品名称、描述、类别作为标签参考）
data = df[["商品名称", "描述", "类别"]].dropna()

# 2. 文本清洗（去除特殊符号、数字等）
def clean_text(text):
    text = re.sub(r"[^\w\s]", "", text)  # 去除标点
    text = re.sub(r"\d+", "", text)  # 去除数字
    return text.strip()

data["清洗后名称"] = data["商品名称"].apply(clean_text)
data["清洗后描述"] = data["描述"].apply(clean_text)

# 3. 合并文本特征（名称+描述更全面）
data["合并文本"] = data["清洗后名称"] + " " + data["清洗后描述"]

# 4. 分词（使用jieba）
def tokenize(text):
    return " ".join(jieba.cut(text))  # 分词后用空格连接

data["分词文本"] = data["合并文本"].apply(tokenize)

# 提取关键词（TopN）
def extract_keywords(text, topk=3):
    return jieba.analyse.extract_tags(text, topK=topk, withWeight=False)

# 对每一行文本提取关键词作为标签
data["挖掘标签"] = data["合并文本"].apply(lambda x: extract_keywords(x, topk=3))

# 查看示例结果
print("标签挖掘示例：")
print(data[["商品名称", "挖掘标签"]].head(5))

# 构建TF-IDF特征矩阵
tfidf = TfidfVectorizer(max_features=1000)
X = tfidf.fit_transform(data["分词文本"])

# 聚类（假设聚成20类，可根据业务调整）
kmeans = KMeans(n_clusters=20, random_state=42)
data["聚类标签"] = kmeans.fit_predict(X)

# 为每个聚类提取核心标签
cluster_tags = {}
for cluster_id in data["聚类标签"].unique():
    cluster_text = " ".join(data[data["聚类标签"] == cluster_id]["分词文本"])
    cluster_tags[cluster_id] = extract_keywords(cluster_text, topk=5)

# 映射聚类结果到标签
data["聚类挖掘标签"] = data["聚类标签"].apply(lambda x: cluster_tags[x])

print("\n聚类标签示例：")
print(data[["商品名称", "聚类挖掘标签"]].head(5))

# 目标标签：商品类别
y = data["类别"]
# 特征：TF-IDF向量
X_tfidf = tfidf.transform(data["分词文本"])  # 复用之前的TF-IDF模型

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(
    X_tfidf, y, test_size=0.2, random_state=42
)

# 初始化模型
model = MultinomialNB()
# 训练
model.fit(X_train, y_train)
# 预测
y_pred = model.predict(X_test)

# 评估模型
print("\n模型评估报告：")
print(classification_report(y_test, y_pred))

def predict_category(new_product_name, new_product_desc):
    # 预处理新商品文本
    text = clean_text(new_product_name + " " + new_product_desc)
    text_tokenized = tokenize(text)
    # 转换为TF-IDF特征
    text_tfidf = tfidf.transform([text_tokenized])
    # 预测类别
    pred = model.predict(text_tfidf)
    # 同时提取关键词标签
    keywords = extract_keywords(text, topk=3)
    return {"预测类别": pred[0], "关键词标签": keywords}

# 测试新商品预测
test_product = {
    "名称": "华为 智能手表 新款",
    "描述": "华为出品智能手表，支持心率监测，适合运动人群"
}
result = predict_category(test_product["名称"], test_product["描述"])
print("\n新商品预测结果：", result)

# 统计Top10高频标签
all_tags = [tag for tags in data["挖掘标签"] for tag in tags]
tag_counts = Counter(all_tags).most_common(10)

# 可视化
tags, counts = zip(*tag_counts)
plt.figure(figsize=(12, 6))
plt.bar(tags, counts)
plt.title("Top10高频挖掘标签")
plt.xticks(rotation=45)
plt.show()

