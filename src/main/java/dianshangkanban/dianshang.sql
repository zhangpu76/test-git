set hive.exec.mode.local.auto=True;
create database aa;
use aa;

-- 创建店铺表
CREATE TABLE shop_pages (
      page_id string COMMENT '页面ID',
      page_type string COMMENT '页面类型（首页、自定义承接页等）',
      page_name string COMMENT '页面名称',
      create_time string COMMENT '创建时间',
      is_active int COMMENT '是否活跃（1-是，0-否）'
)COMMENT'店铺页面表';

-- 创建访问记录表
CREATE TABLE page_visits (
      visit_id string COMMENT '访问记录ID',
      page_id string COMMENT '关联的页面ID',
      visitor_id string COMMENT '访问者ID',
      visit_time string COMMENT '访问时间',
      visit_duration string COMMENT '访问时长（秒）',
      is_new_visitor string COMMENT '是否新访客（1-是，0-否）',
      referrer_type string COMMENT '来源类型（如直接访问、搜索、外部链接等）',
      referrer_value string COMMENT '来源值（如搜索关键词、外部链接URL等）',
      click_count string COMMENT '点击次数',
      scroll_depth string COMMENT '页面滚动深度（百分比）',
      FOREIGN KEY (page_id) REFERENCES shop_pages(page_id)
) COMMENT'页面访问记录表';

-- 插入模拟店铺页面数据
INSERT INTO shop_pages (page_type, page_name) VALUES
      ('首页', '天猫旗舰店首页'),
      ('自定义承接页', '618大促活动页'),
      ('自定义承接页', '双11精选商品页'),
      ('首页', '京东自营店首页'),
      ('自定义承接页', '新品首发活动页');

-- 插入模拟访问记录数据（为简化示例，仅展示部分记录）
INSERT INTO page_visits (page_id, visitor_id, visit_time, visit_duration, is_new_visitor, referrer_type, referrer_value, click_count, scroll_depth) VALUES
      (1, 'U1001', '2025-07-28 09:15:30', 120, 1, '搜索引擎', '商品关键词', 5, 75.25),
      (1, 'U1002', '2025-07-28 10:30:45', 85, 0, '社交媒体', '微信分享', 3, 60.00),
      (2, 'U1003', '2025-07-28 11:45:10', 210, 1, '外部链接', '淘宝广告', 8, 90.50),
      (3, 'U1001', '2025-07-28 14:20:15', 150, 0, '直接访问', NULL, 6, 80.75),
      (4, 'U1004', '2025-07-28 15:30:20', 90, 1, '搜索引擎', '品牌名', 4, 65.20),
      (2, 'U1005', '2025-07-28 16:45:30', 180, 1, '社交媒体', '微博推广', 7, 85.00);

-- 查询店铺页面的点击、访问等数据情况（按页面类型统计）
SELECT
    sp.page_type,
    COUNT(DISTINCT pv.visitor_id) AS total_visitors,  -- 总访客数（去重）
    COUNT(pv.visit_id) AS total_visits,  -- 总访问次数
    SUM(pv.click_count) AS total_clicks,  -- 总点击次数
    AVG(pv.visit_duration) AS avg_visit_duration,  -- 平均访问时长
    SUM(CASE WHEN pv.is_new_visitor = 1 THEN 1 ELSE 0 END) AS new_visitors,  -- 新访客数
    SUM(CASE WHEN pv.is_new_visitor = 0 THEN 1 ELSE 0 END) AS returning_visitors,  -- 老访客数
    ROUND(SUM(pv.click_count) / COUNT(pv.visit_id), 2) AS clicks_per_visit,  -- 每次访问平均点击数
    ROUND(AVG(pv.scroll_depth), 2) AS avg_scroll_depth  -- 平均页面滚动深度
FROM
    shop_pages sp
        LEFT JOIN
    page_visits pv ON sp.page_id = pv.page_id
WHERE
        sp.is_active = 1  -- 只统计活跃页面
  AND pv.visit_time >= '2025-07-01'  -- 限制时间范围（示例为7月数据）
  AND pv.visit_time < '2025-08-01'
GROUP BY
    sp.page_type
ORDER BY
    total_visitors DESC;  -- 按访客数降序排列

-- 查询特定页面的详细访问数据（示例：查询ID为2的自定义承接页）
SELECT
    DATE(pv.visit_time) AS visit_date,
    COUNT(DISTINCT pv.visitor_id) AS daily_visitors,
    COUNT(pv.visit_id) AS daily_visits,
    SUM(pv.click_count) AS daily_clicks,
    AVG(pv.visit_duration) AS avg_daily_duration,
    ROUND(AVG(pv.scroll_depth), 2) AS avg_daily_scroll
FROM
    page_visits pv
WHERE
        pv.page_id = 2  -- 特定页面ID
  AND pv.visit_time >= '2025-07-01'
  AND pv.visit_time < '2025-08-01'
GROUP BY
    DATE(pv.visit_time)
ORDER BY
    visit_date ASC;  -- 按日期升序排列