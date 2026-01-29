package com.bhcode.flare.common.util;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.RecordComponent;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DBUtils {

    private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Field[]> FIELDS_CACHE = new ConcurrentHashMap<>();
    private static final Map<Class<?>, RecordComponent[]> RECORD_COMPONENTS_CACHE = new ConcurrentHashMap<>();

    /**
     * Convert ResultSet to a list of objects (supports both POJOs and Records, and nested mapping).
     */
    public static <T> List<T> resultSetToBeanList(ResultSet rs, Class<T> clazz) throws Exception {
        List<T> list = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Map<String, Object> rowData = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                // Use column label (alias) as the key
                rowData.put(metaData.getColumnLabel(i), rs.getObject(i));
            }

            if (clazz.isRecord()) {
                list.add(mapToRecord(rowData, clazz));
            } else {
                list.add(mapToPojo(rowData, clazz));
            }
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    private static <T> T mapToRecord(Map<String, Object> rowData, Class<T> clazz) throws Exception {
        Constructor<?> canonical = CONSTRUCTOR_CACHE.computeIfAbsent(clazz, k -> {
            Constructor<?>[] constructors = k.getDeclaredConstructors();
            return constructors[0];
        });
        
        RecordComponent[] components = RECORD_COMPONENTS_CACHE.computeIfAbsent(clazz, Class::getRecordComponents);
        
        Object[] args = new Object[components.length];
        for (int i = 0; i < components.length; i++) {
            String name = components[i].getName();
            Class<?> type = components[i].getType();
            
            // Try direct match, snake_case match, and nested match
            Object val = getValue(rowData, name, type);
            args[i] = val;
        }
        
        canonical.setAccessible(true);
        return (T) canonical.newInstance(args);
    }

    @SuppressWarnings("unchecked")
    private static <T> T mapToPojo(Map<String, Object> rowData, Class<T> clazz) throws Exception {
        Constructor<?> constructor = CONSTRUCTOR_CACHE.computeIfAbsent(clazz, k -> {
            try {
                return k.getDeclaredConstructor();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        });
        
        Field[] fields = FIELDS_CACHE.computeIfAbsent(clazz, Class::getDeclaredFields);
        
        constructor.setAccessible(true);
        T obj = (T) constructor.newInstance();
        for (Field field : fields) {
            field.setAccessible(true);
            String name = field.getName();
            Class<?> type = field.getType();
            
            Object val = getValue(rowData, name, type);
            if (val != null) {
                field.set(obj, val);
            }
        }
        return obj;
    }

    /**
     * Get value from rowData with support for camelCase, snake_case, and nested structures.
     */
    private static Object getValue(Map<String, Object> rowData, String name, Class<?> type) throws Exception {
        // 1. Direct match
        if (rowData.containsKey(name)) return rowData.get(name);
        
        // 2. snake_case match
        String snakeName = toSnakeCase(name);
        if (rowData.containsKey(snakeName)) return rowData.get(snakeName);
        
        // 3. Nested match (e.g., field "user" of type UserRecord, looking for "user.xxx" or "user_xxx" in rowData)
        if (!isSimpleType(type)) {
            Map<String, Object> nestedData = new HashMap<>();
            String prefix1 = name + ".";
            String prefix2 = name + "_";
            String prefix3 = snakeName + "_";
            
            for (Map.Entry<String, Object> entry : rowData.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(prefix1)) {
                    nestedData.put(key.substring(prefix1.length()), entry.getValue());
                } else if (key.startsWith(prefix2)) {
                    nestedData.put(key.substring(prefix2.length()), entry.getValue());
                } else if (key.startsWith(prefix3)) {
                    nestedData.put(key.substring(prefix3.length()), entry.getValue());
                }
            }
            
            if (!nestedData.isEmpty()) {
                if (type.isRecord()) {
                    return mapToRecord(nestedData, (Class) type);
                } else {
                    return mapToPojo(nestedData, (Class) type);
                }
            }
        }
        
        return null;
    }

    private static boolean isSimpleType(Class<?> type) {
        return type.isPrimitive() || 
               type == String.class || 
               type == Integer.class || 
               type == Long.class || 
               type == Double.class || 
               type == Float.class || 
               type == Boolean.class || 
               type == Short.class || 
               type == Byte.class || 
               type == java.math.BigDecimal.class ||
               type == java.sql.Date.class ||
               type == java.sql.Timestamp.class ||
               type == java.util.Date.class;
    }

    /**
     * Convert camelCase to snake_case.
     */
    public static String toSnakeCase(String str) {
        if (str == null) return null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) sb.append("_");
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
