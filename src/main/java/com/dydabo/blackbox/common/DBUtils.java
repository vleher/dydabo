/*
 * Copyright 2017 viswadas leher .
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.dydabo.blackbox.common;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @param <T>
 * @author viswadas leher
 */
public interface DBUtils<T extends BlackBoxable> {

    /**
     * @param row
     * @return
     */
    default GenericDBTableRow convertRowToTableRow(T row) {
        GenericDBTableRow cTable = new GenericDBTableRow(row.getBBRowKey());
        Map<String, Field> fields = DyDaBoUtils.getFieldFromType(row.getClass());

        fields.forEach((key, field) -> {
            try {
                if (!field.isSynthetic()) {
                    field.setAccessible(true);
                    cTable.getDefaultFamily().addColumn(key, field.get(row));
                }
            } catch (IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(CassandraUtils.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        return cTable;
    }
}
