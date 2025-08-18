package com.stream.common.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Appender;
import java.util.Enumeration;
public class LogUtils {
    // 关闭所有日志输出
    public static void disableAllLogging() {
        // 1. 获取根日志器
        Logger rootLogger = Logger.getRootLogger();

        // 2. 移除所有输出器（Appender）
        Enumeration<Appender> appenders = rootLogger.getAllAppenders();
        while (appenders.hasMoreElements()) {
            rootLogger.removeAppender(appenders.nextElement());
        }

        // 3. 将所有日志级别设置为OFF（最高级别，完全关闭）
        rootLogger.setLevel(Level.OFF);

        // 4. 额外处理常见包的日志（确保覆盖所有可能输出日志的组件）
        Logger.getLogger("org.apache.flink").setLevel(Level.OFF);
        Logger.getLogger("com.ververica.cdc").setLevel(Level.OFF);
        Logger.getLogger("org.apache.kafka").setLevel(Level.OFF);
        // 可根据实际日志输出的包名继续添加
    }
}
