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
 *
 * @author viswadas leher
 */
public interface BlackBox<T extends BlackBoxable> {

    /**
     *
     * @param row
     * @return
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    boolean delete(List<T> row) throws BlackBoxException;

    /**
     *
     * @param row
     * @return
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    boolean insert(List<T> row) throws BlackBoxException;

    /**
     *
     * @param row
     * @return
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    List<T> select(List<T> row) throws BlackBoxException;

    /**
     *
     * @param oldRow
     * @param newRow
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    boolean update(List<T> newRow) throws BlackBoxException;

}
