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

/**
 *
 * The interface that needs to be implemented by the POJO that needs to be saved into the Database.
 *
 * @author viswadas leher
 */
public interface BlackBoxable {

    /**
     * A Json representation of the POJO
     *
     * @return a valid Json string
     */
    String getBBJson();

    /**
     * Create a row key that will be used to store the object to the database
     *
     * @return the row key or an unique identifier
     */
    String getBBRowKey();

}
