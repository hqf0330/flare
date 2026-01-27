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
     * Convert ResultSet to a list of objects (supports both POJOs and Records).
     */
    public static <T> List<T> resultSetToBeanList(ResultSet rs, Class<T> clazz) throws Exception {
        List<T> list = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Map<String, Object> rowData = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(metaData.getColumnLabel(i).toLowerCase(), rs.getObject(i));
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
            String name = components[i].getName().toLowerCase();
            args[i] = rowData.get(name);
        }
        
        canonical.setAccessible(true);
        return (T) canonical.newInstance(args);
    }

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
            String name = field.getName().toLowerCase();
            if (rowData.containsKey(name)) {
                field.set(obj, rowData.get(name));
            }
        }
        return obj;
    }
}
