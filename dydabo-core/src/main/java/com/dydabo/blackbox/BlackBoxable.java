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
package com.dydabo.blackbox;

import java.io.Serializable;

/**
 * The interface that needs to be implemented by the POJO that needs to be saved into the Database.
 *
 * @author viswadas leher
 */
public interface BlackBoxable extends Serializable {

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
