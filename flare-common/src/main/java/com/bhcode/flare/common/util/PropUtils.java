package com.bhcode.flare.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PropUtils {

    private static final Map<String, String> settings = new ConcurrentHashMap<>();
    private static final java.util.Set<String> alreadyLoadedFiles = java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * 加载配置文件
     *
     * @param fileNames 配置文件名称（不需要 .properties 后缀）
     */
    public static void load(String... fileNames) {
        if (fileNames == null || fileNames.length == 0) {
            return;
        }

        for (String fileName : fileNames) {
            if (StringUtils.isBlank(fileName)) {
                continue;
            }

            String fullName = fileName.endsWith(".properties") ? fileName : fileName + ".properties";

            // 避免重复加载
            if (!alreadyLoadedFiles.add(fullName)) {
                continue;
            }

            try (InputStream is = PropUtils.class.getClassLoader().getResourceAsStream(fullName)) {
                if (is != null) {
                    log.debug("Loading configuration file: {}", fullName);
                    Properties props = new Properties();
                    props.load(is);
                    // 将配置信息存放到 settings 中
                    props.stringPropertyNames().forEach(key -> {
                        String value = props.getProperty(key);
                        settings.put(key, value);
                    });
                    log.debug("Loaded configuration file: {}", fullName);
                } else {
                    log.debug("Configuration file not found: {} (this is normal if using annotations)", fullName);
                }
            } catch (Exception e) {
                log.warn("Failed to load configuration file: {}", fullName, e);
            }
        }
    }

    /**
     * 获取配置值
     *
     * @param key 配置键
     * @return 配置值，如果不存在返回 null
     */
    public static String getString(String key) {
        return settings.get(key);
    }

    /**
     * 获取配置值，如果不存在返回默认值
     *
     * @param key          配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public static String getString(String key, String defaultValue) {
        String value = settings.get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取整型配置值
     *
     * @param key          配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public static int getInt(String key, int defaultValue) {
        String value = getString(key);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Failed to parse int value for key: {}, using default: {}", key, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 获取长整型配置值
     *
     * @param key          配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public static long getLong(String key, long defaultValue) {
        String value = getString(key);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Failed to parse long value for key: {}, using default: {}", key, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 获取布尔型配置值
     *
     * @param key          配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = getString(key);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }

    /**
     * 设置配置属性
     *
     * @param key   配置键
     * @param value 配置值
     */
    public static void setProperty(String key, String value) {
        if (StringUtils.isNotBlank(key) && value != null) {
            settings.put(key, value);
        }
    }

    /**
     * 获取所有配置信息
     *
     * @return 配置信息 Map
     */
    public static Map<String, String> getSettings() {
        return new HashMap<>(settings);
    }

    /**
     * 获取指定前缀的所有配置
     *
     * @param prefix 配置前缀
     * @return 配置信息 Map（key 已去除前缀）
     */
    public static Map<String, String> sliceKeys(String prefix) {
        Map<String, String> result = new HashMap<>();
        settings.forEach((key, value) -> {
            if (key != null && key.startsWith(prefix)) {
                String newKey = key.substring(prefix.length());
                result.put(newKey, value);
            }
        });
        return result;
    }

    /**
     * 加载 SQL 配置（从类上的注解或配置文件）
     *
     * @param clazz 任务类
     * @return SQL 配置 Map
     */
    public static Map<String, String> loadSqlConfig(Class<?> clazz) {
        // TODO: 实现从注解或配置文件加载 SQL 配置
        return new HashMap<>();
    }

    private PropUtils() {
        // Private constructor
    }
}
