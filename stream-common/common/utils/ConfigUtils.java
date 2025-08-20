package com.stream.common.utils;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static Properties properties;

    static {
        try {
            properties = new Properties();
            properties.load(ConfigUtils.class.getClassLoader().getResourceAsStream("common-config.properties"));
        } catch (IOException e) {
            logger.error("加载配置文件出错, exit 1", e);
            System.exit(1);
        }
    }

    public static String getString(String key) {
        // logger.info("加载配置[" + key + "]:" + value);
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new RuntimeException("配置项不存在或为空：" + key); // 主动抛错，明确提示缺失的配置
            // 或返回默认值：return "默认值";
        }
        return value;
    }

    public static int getInt(String key) {
        String value = properties.getProperty(key).trim();
        return Integer.parseInt(value);
    }

    public static int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty(value) ? defaultValue : Integer.parseInt(value);
    }

    public static long getLong(String key) {
        String value = properties.getProperty(key).trim();
        return Long.parseLong(value);
    }

    public static long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty(value) ? defaultValue : Long.parseLong(value);
    }

    private static final Properties props = new Properties();
    static {
        try {
            String fileName = "common-config.properties";
            InputStream is = ConfigUtils.class.getClassLoader().getResourceAsStream(fileName);
            if (is == null) {
                // 打印类路径，辅助排查资源位置
                System.out.println("类路径：" + System.getProperty("java.class.path"));
                throw new RuntimeException("配置文件 " + fileName + " 未找到");
            }
            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("加载配置文件失败", e);
        }
    }
}
