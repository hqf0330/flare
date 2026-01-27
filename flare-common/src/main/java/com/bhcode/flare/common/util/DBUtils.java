package com.bhcode.flare.common.util;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DBUtils {

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

    private static <T> T mapToRecord(Map<String, Object> rowData, Class<T> clazz) throws Exception {
        Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        // Record usually has one canonical constructor
        Constructor<?> canonical = constructors[0];
        Class<?>[] paramTypes = canonical.getParameterTypes();
        Object[] args = new Object[paramTypes.length];
        
        // Get record components
        java.lang.reflect.RecordComponent[] components = clazz.getRecordComponents();
        for (int i = 0; i < components.length; i++) {
            String name = components[i].getName().toLowerCase();
            args[i] = rowData.get(name);
        }
        
        canonical.setAccessible(true);
        return (T) canonical.newInstance(args);
    }

    private static <T> T mapToPojo(Map<String, Object> rowData, Class<T> clazz) throws Exception {
        T obj = clazz.getDeclaredConstructor().newInstance();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String name = field.getName().toLowerCase();
            if (rowData.containsKey(name)) {
                field.set(obj, rowData.get(name));
            }
        }
        return obj;
    }
}
