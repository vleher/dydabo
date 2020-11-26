/*
 *  Copyright 2020 viswadas leher <vleher@gmail.com>.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.dydabo.blackbox;

import java.util.List;

/**
 * The BlackBox interface allows you to insert, update, delete, search and fetch objects from the underlying database.
 *
 * @param <T>
 * @author viswadas leher
 */
public interface BlackBox<T extends BlackBoxable> {

    /**
     * Delete a list of POJO beans that are
     * rows in the table. Each of these objects should have a valid row key.
     *
     * @param rows a list of POJO
     * @return true if all deletes have been successful, false otherwise
     * @throws BlackBoxException blackbox exception
     */
    boolean delete(List<T> rows) throws BlackBoxException;

    /**
     * @param row row to insert
     * @return true if delete is successful
     * @throws BlackBoxException blackbox exception
     */
    boolean delete(T row) throws BlackBoxException;

    /**
     * Get all matching rows given a list of row keys.
     *
     * @param  rows list of rows
     * @return a list of POJO that match the keys
     * @throws BlackBoxException blackbox exception
     */
    List<T> fetch(List<T> rows) throws BlackBoxException;

    /**
     * @param row the row to fetch
     * @return list of POJO matching the key
     * @throws BlackBoxException blackbox exception
     */
    List<T> fetch(T row) throws BlackBoxException;

    /**
     * Get all matching rows given a list of partial keys. This uses the fuzzy filter to search through keys
     *
     * @param rows list of partial keys in regex format
     * @return a list of POJO that match the keys
     * @throws BlackBoxException black box exception
     */
    List<T> fetchByPartialKey(List<T> rows) throws BlackBoxException;

    /**
     * Get matching rows upto maxResults given a list of partial keys. This uses the fuzzy filter to search through keys
     *
     * @param rows    list of partial keys in regex format
     * @param maxResults maximum number of results returned
     * @return a list of POJO that match the keys
     * @throws BlackBoxException blackbox exception
     */
    List<T> fetchByPartialKey(List<T> rows, long maxResults) throws BlackBoxException;

    /**
     * Get all matching rows given a partial key.
     *
     * @param row key in regex format
     * @return a list of POJO that match the key
     * @throws BlackBoxException blackbox exception
     */
    List<T> fetchByPartialKey(T row) throws BlackBoxException;

    /**
     * Get matching rows upto maxResults given a list of partial keys. This uses the fuzzy filter to search through keys
     *
     * @param row     a key in regex format
     * @param maxResults maximum number of results returned
     * @return a list of POJO that match the keys
     * @throws BlackBoxException blackbox exception
     */
    List<T> fetchByPartialKey(T row, long maxResults) throws BlackBoxException;

    /**
     * Inserts a list of rows into the table. The rows should not exist in the table.
     *
     * @param rows a list of POJO
     * @return true if inserts are successful, false otherwise
     * @throws BlackBoxException blackbox exception
     */
    boolean insert(List<T> rows) throws BlackBoxException;

    /**
     * Inserts a row into the table. The row should not exist in the table.
     *
     * @param row a list of POJO
     * @return true if inserts are successful, false otherwise
     * @throws BlackBoxException blackbox exception
     */
    boolean insert(T row) throws BlackBoxException;

    /**
     * Search for all matching rows in the table, given a list of POJO. You can add known values to the POJO fields or regular
     * expressions in the field.
     * <p>
     * Example: to search for all names starting with David, you can do obj.setName("David.*") and pass in the obj to the
     * search method.
     *
     * @param rows a list of POJO
     * @return list of POJO that match the criteria
     * @throws BlackBoxException blackbox exception
     */
    List<T> search(List<T> rows) throws BlackBoxException;

    /**
     * Search for all matching rows in the table, given a list of POJO. You can add known values to the POJO fields or regular
     * expressions in the field.
     * <p>
     * Example: to search for all names starting with David, you can do obj.setName("David.*") and pass in the obj to the
     * search method.
     *
     * @param rows       a list of POJO
     * @param maxResults the maximum number of results to return
     * @return list of POJO that match the criteria
     * @throws BlackBoxException blackbox exception
     */
    List<T> search(List<T> rows, long maxResults) throws BlackBoxException;

    /**
     * Search for all matching rows in the table, given a POJO. You can add known values to the POJO fields or regular
     * expressions in the field.
     * <p>
     * Example: to search for all names starting with David, you can do obj.setName("David.*") and pass in the obj to the
     * search method.
     *
     * @param row a POJO
     * @return list of POJO that match the criteria
     * @throws BlackBoxException blackbox exception
     */
    List<T> search(T row) throws BlackBoxException;

    /**
     * Search for all matching rows in the table, given a POJO. You can add known values to the POJO fields or regular
     * expressions in the field.
     * <p>
     * Example: to search for all names starting with David, you can do obj.setName("David.*") and pass in the obj to the
     * search method.
     *
     * @param row        a POJO
     * @param maxResults the maximum number of results to return
     * @return list of POJO that match the criteria
     * @throws BlackBoxException blackbox exception
     */
    List<T> search(T row, long maxResults) throws BlackBoxException;

    /**
     * Search for all matching rows in the table, given a two POJOs. You can add known values to the POJO fields or regular
     * expressions in the field. The two POJO act as the start and end filters for the results
     *
     * @param startRow the start filter POJO
     * @param endRow   the end filter POJO
     * @return a list of POJO matching the criteria
     * @throws BlackBoxException blackbox exception
     */
    List<T> search(T startRow, T endRow) throws BlackBoxException;

    /**
     * Search for all matching rows in the table, given a two POJOs. You can add known values to the POJO fields or regular
     * expressions in the field. The two POJO act as the start and end filters for the results
     *
     * @param startRow   the start filter POJO
     * @param endRow     the end filter POJO
     * @param maxResults the maximum number of results
     * @return a list of POJO matching the criteria
     * @throws BlackBoxException blackbox exception
     */
    List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException;

    /**
     * Update or insert the rows that match the POJO. If a row does not exist, then it will added otherwise the existing row
     * is updated with the new values.
     *
     * @param newRows a list of POJO
     * @return true if update is successful, false otherwise
     * @throws BlackBoxException blackbox exception
     */
    boolean update(List<T> newRows) throws BlackBoxException;

    /**
     * Update or insert the row that match the POJO. If a row does not exist, then it will added otherwise the existing row
     * is updated with the new values.
     *
     * @param newRow a POJO
     * @return true if update is successful, false otherwise
     * @throws BlackBoxException blackbox exception
     */
    boolean update(T newRow) throws BlackBoxException;

}
