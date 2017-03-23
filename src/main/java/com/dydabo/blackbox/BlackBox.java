/*
 * Copyright (C) 2017 viswadas leher
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
     * Get all matching rows given a list of row keys.
     *
     * @param rowKeys list of row keys
     * @param bean    the POJO to return
     *
     * @returna list of POJO that match the keys
     *
     * @throws BlackBoxException
     */
    List<T> fetch(List<String> rowKeys, T bean) throws BlackBoxException;

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

}
