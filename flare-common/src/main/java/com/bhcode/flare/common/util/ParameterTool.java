package com.bhcode.flare.common.util;

import java.util.*;

public class ParameterTool {
    
    private static final String NO_VALUE_KEY = "__NO_VALUE_KEY";
    private final Map<String, String> data;

    private ParameterTool(Map<String, String> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data));
    }

    /**
     * 从命令行参数创建 ParameterTool
     *
     * @param args 命令行参数数组
     * @return ParameterTool 实例
     */
    public static ParameterTool fromArgs(String[] args) {
        if (args == null || args.length == 0) {
            return fromMap(Collections.emptyMap());
        }

        Map<String, String> map = new HashMap<>(args.length / 2);
        int i = 0;
        
        while (i < args.length) {
            String arg = args[i];
            
            // 检查是否是参数名（以 - 或 -- 开头）
            if (arg.startsWith("--")) {
                String key = arg.substring(2);
                if (key.isEmpty()) {
                    throw new IllegalArgumentException("参数名不能为空: " + arg);
                }
                
                i++;
                // 检查下一个参数是否是值
                if (i < args.length && !args[i].startsWith("-")) {
                    map.put(key, args[i]);
                    i++;
                } else {
                    map.put(key, NO_VALUE_KEY);
                }
            } else if (arg.startsWith("-")) {
                String key = arg.substring(1);
                if (key.isEmpty()) {
                    throw new IllegalArgumentException("参数名不能为空: " + arg);
                }
                
                i++;
                // 检查下一个参数是否是值
                if (i < args.length && !args[i].startsWith("-")) {
                    map.put(key, args[i]);
                    i++;
                } else {
                    map.put(key, NO_VALUE_KEY);
                }
            } else {
                throw new IllegalArgumentException(
                    "参数必须以 '-' 或 '--' 开头: " + arg + 
                    ". 参数列表: " + Arrays.toString(args));
            }
        }

        return fromMap(map);
    }

    /**
     * 从 Map 创建 ParameterTool
     *
     * @param map 参数 Map
     * @return ParameterTool 实例
     */
    public static ParameterTool fromMap(Map<String, String> map) {
        return new ParameterTool(map);
    }

    /**
     * 获取参数值
     *
     * @param key 参数名
     * @return 参数值，如果不存在或没有值返回 null
     */
    public String get(String key) {
        String value = data.get(key);
        return NO_VALUE_KEY.equals(value) ? null : value;
    }

    /**
     * 获取参数值，如果不存在返回默认值
     *
     * @param key          参数名
     * @param defaultValue 默认值
     * @return 参数值或默认值
     */
    public String get(String key, String defaultValue) {
        String value = data.get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 检查参数是否存在
     *
     * @param key 参数名
     * @return true: 存在  false: 不存在
     */
    public boolean has(String key) {
        return data.containsKey(key);
    }

    /**
     * 获取所有参数
     *
     * @return 参数 Map 的副本
     */
    public Map<String, String> toMap() {
        return new HashMap<>(data);
    }
}
