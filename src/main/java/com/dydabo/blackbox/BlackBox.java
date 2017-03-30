/** *****************************************************************************
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 */
package com.dydabo.blackbox;

import java.util.List;

/**
 * The BlackBox interface allows you to insert, update, delete, search and fetch Objects from the Underlying database.
 *
 * @author viswadas leher
 * @param <T>
 */
public interface BlackBox<T extends BlackBoxable> {

    /**
     * Delete a list of POJO beans that are rows in the table. Each of these objects should have a valid row key.
     *
     * @param rows a list of POJO
     *
     * @return true if all deletes have been successful, false otherwise
     *
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    boolean delete(List<T> rows) throws BlackBoxException;

    /**
     *
     * @param row
     *
     * @return
     *
     * @throws BlackBoxException
     */
    boolean delete(T row) throws BlackBoxException;

    /**
     * Get all matching rows given a list of row keys.
     *
     * @param rowKeys list of row keys
     * @param bean    the POJO to return
     *
     * @return
     *
     * @returna list of POJO that match the keys
     *
     * @throws BlackBoxException
     */
    List<T> fetch(List<String> rowKeys, T bean) throws BlackBoxException;

    /**
     *
     * @param rowKey
     * @param bean
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> fetch(String rowKey, T bean) throws BlackBoxException;

    /**
     * Get all matching rows given a list of partial keys. This uses the fuzzy filter to search through keys
     *
     * @param rowKeys list of partial keys in regex format
     * @param bean    the POJO type to return
     *
     * @return a list of POJO that match the keys
     *
     * @throws BlackBoxException
     */
    List<T> fetchByPartialKey(List<String> rowKeys, T bean) throws BlackBoxException;

    /**
     *
     * @param rowKeys
     * @param bean
     * @param maxResults
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> fetchByPartialKey(List<String> rowKeys, T bean, int maxResults) throws BlackBoxException;

    /**
     *
     * @param rowKey
     * @param bean
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> fetchByPartialKey(String rowKey, T bean) throws BlackBoxException;

    /**
     *
     * @param rowKey
     * @param bean
     * @param maxResults
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> fetchByPartialKey(String rowKey, T bean, int maxResults) throws BlackBoxException;

    /**
     * Inserts a list of rows into the table. The rows should not already exist in the table.
     *
     * @param rows a list of POJO
     *
     * @return true if inserts are successful, false otherwise
     *
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    boolean insert(List<T> rows) throws BlackBoxException;

    /**
     *
     * @param row
     *
     * @return
     *
     * @throws BlackBoxException
     */
    boolean insert(T row) throws BlackBoxException;

    /**
     * Search for all matching rows in the table, given a list of POJO. You can add known values to the POJO fields or regular
     * expressions in the field.
     *
     * Example: to search for all names starting with David, you can do obj.setName("David.*") and pass in the obj to the
     * search method.
     *
     * @param rows a list of POJO
     *
     * @return list of POJO that match the criteria
     *
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    List<T> search(List<T> rows) throws BlackBoxException;

    /**
     *
     * @param rows
     * @param maxResults
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> search(List<T> rows, int maxResults) throws BlackBoxException;

    /**
     *
     * @param row
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> search(T row) throws BlackBoxException;

    /**
     *
     * @param row
     * @param maxResults
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> search(T row, int maxResults) throws BlackBoxException;

    /**
     *
     * @param startRow
     * @param endRow
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> search(T startRow, T endRow) throws BlackBoxException;

    /**
     *
     * @param startRow
     * @param endRow
     * @param maxResults
     *
     * @return
     *
     * @throws BlackBoxException
     */
    List<T> search(T startRow, T endRow, int maxResults) throws BlackBoxException;

    /**
     * Update or insert the rows that match the POJO. If a row does not exist, then it will added otherwise the existing row
     * is updated with the new values.
     *
     * @param newRows a list of POJO
     *
     * @return true if update is successful, false otherwise
     *
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    boolean update(List<T> newRows) throws BlackBoxException;

    /**
     *
     * @param newRow
     *
     * @return
     *
     * @throws BlackBoxException
     */
    boolean update(T newRow) throws BlackBoxException;

}
