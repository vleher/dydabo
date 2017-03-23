/*
 * Copyright (C) 2017 viswadas leher <vleher@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.hbase.obj.HBaseTable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseUtils<T extends BlackBoxable> {

    private static SortedSet tableCache = new TreeSet();

    public T generateObjectFromMap(HashMap<String, Object> valueTable, T row) throws BlackBoxException {

        GsonBuilder gsonBuilder = new GsonBuilder();
        //gsonBuilder.registerTypeAdapter(row.getClass(), new GenericClassDeserializer<T>());

        Gson gson = gsonBuilder.create();

        try {
            System.out.println("Generate Object from:" + valueTable);

            Constructor<?>[] publicConstructors = row.getClass().getConstructors();
            if (publicConstructors.length == 0) {
                throw new BlackBoxException("No public constructors found in class " + row);
            }

            T resultObject = null;

            for (Constructor<?> publicConstructor : publicConstructors) {
                if (publicConstructor.getParameterCount() == 0) {
                    try {
                        resultObject = (T) publicConstructor.newInstance();
                        break;
                    } catch (InstantiationException ex) {
                        Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IllegalAccessException ex) {
                        Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IllegalArgumentException ex) {
                        Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (InvocationTargetException ex) {
                        Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

                if (resultObject == null) {
                    try {
                        List<Object> args = new ArrayList<>();
                        for (int i = 0; i < publicConstructor.getParameterCount(); i++) {
                            args.add(null);
                        }
                        resultObject = (T) publicConstructor.newInstance(args.toArray());
                        break;
                    } catch (InstantiationException ex) {
                        Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IllegalAccessException ex) {
                        Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IllegalArgumentException ex) {
                        Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (InvocationTargetException ex) {
                        Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }

            if (publicConstructors.length == 0) {
                throw new BlackBoxException("No suitable constructors found in class " + row);
            }

            JsonObject json = new JsonObject();

            BeanInfo beanInfo = Introspector.getBeanInfo(row.getClass(), Object.class);
            for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
                Method writeMethod = propertyDescriptor.getWriteMethod();
                if (writeMethod != null) {
                    System.out.println("Write Method:" + writeMethod);
                    Class<?>[] paramTypes = writeMethod.getParameterTypes();
                    // We recognize only setter methods with exactly one parameter
                    if (paramTypes.length == 1) {
                        Class thisType = paramTypes[0];

                        final Object propValue = valueTable.get(writeMethod.getName().substring(3));
                        System.out.println(writeMethod.getName() + " :" + thisType + " :" + propValue);
                        if (thisType.isAssignableFrom(String.class)) {
                            System.out.println("Assigning :" + writeMethod);
                            writeMethod.invoke(resultObject, propValue);
                        }
                    }
                }
            }

            for (Map.Entry<String, Object> entry : valueTable.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                System.out.println("Key:" + key + " => " + value + " :" + value.getClass());

            }
            System.out.println("Result Object :" + resultObject);
            return resultObject;
        } catch (IntrospectionException ex) {
            Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SecurityException ex) {
            Logger.getLogger(HBaseUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public TableName getTableName(T row) {
        final String fullClassName = row.getClass().toString().substring(6).replaceAll("\\.", "");
        return TableName.valueOf(fullClassName);
    }

    /**
     *
     * @param row   the value of row
     * @param admin the value of admin
     *
     * @return the boolean
     *
     * @throws java.io.IOException
     */
    public boolean createTable(T row, Connection connection) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            Object lockObject = new Object();
            TableName tableName = getTableName(row);

            if (tableCache.contains(tableName)) {
                return true;
            }

            if (admin.tableExists(tableName)) {
                tableCache.add(tableName);
                return true;
            }

            synchronized (lockObject) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

                HBaseTable hTable = convertJsonToMap(row, true);
                for (HBaseTable.ColumnFamily value : hTable.getColumnFamilies().values()) {
                    HColumnDescriptor dFamily = new HColumnDescriptor(value.getFamilyName());
                    tableDescriptor.addFamily(dFamily);
                }
                admin.createTable(tableDescriptor);
                tableCache.add(tableName);
            }
            return true;
        }
    }

    public HBaseTable convertJsonToMap(T row, boolean includeObject) throws JsonSyntaxException {
        Map<String, Object> thisValueMap = new Gson().fromJson(row.getBBJson(), Map.class);
        System.out.println("New Map:" + thisValueMap);
        HBaseTable hbaseTable = new HBaseTable(row.getBBRowKey());

        for (Map.Entry<String, Object> entry : thisValueMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (isPrimitiveOrPrimitiveWrapperOrString(value)) {
                System.out.println("Primitive :" + value);
                hbaseTable.getDefaultFamily().addColumn(key, value);
            } else if (includeObject) {
                System.out.println("key:" + key + " :" + value + " :" + value.getClass());
                hbaseTable.createFamily(key).addColumn(key, value);
            }
        }

        System.out.println("HbaseTable:" + hbaseTable);
        return hbaseTable;
    }

    /**
     *
     * @param row    the value of row
     * @param hTable the value of hTable
     *
     * @return the boolean
     */
    public boolean checkIfRowExists(T row, Table hTable) throws IOException {
        Get get = new Get(Bytes.toBytes(row.getBBRowKey()));
        Result result = hTable.get(get);
        if (result.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    /**
     *
     * @param type the value of type
     *
     * @return the boolean
     */
    public static boolean isPrimitiveOrPrimitiveWrapperOrString(Object obj) {
        Class<?> type = obj.getClass();
        return (type.isPrimitive() && type != void.class) ||
                type == Double.class || type == Float.class || type == Long.class ||
                type == Integer.class || type == Short.class || type == Character.class ||
                type == Byte.class || type == Boolean.class || type == String.class;
    }

}
