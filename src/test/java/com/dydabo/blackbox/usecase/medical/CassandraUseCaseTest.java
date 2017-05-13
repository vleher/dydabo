/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
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

package com.dydabo.blackbox.usecase.medical;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxFactory;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author viswadas leher
 */
public class CassandraUseCaseTest extends MedicalUseCaseTest {

    protected Logger logger = Logger.getLogger(CassandraUseCaseTest.class.getName());

    /**
     * @throws BlackBoxException
     * @throws IOException
     */
    public CassandraUseCaseTest() throws BlackBoxException, IOException {
        super();
        if (utils.dbToTest.contains(BlackBoxFactory.CASSANDRA)) {
            instance = BlackBoxFactory.getCassandraDatabase("127.0.0.1");
            // Pre-populate with some dynamic data.
            utils.generatePatients(random.nextInt(98765) % 10, instance);
            utils.generateEncounters(random.nextInt(98765) % 10, instance);
        }
    }


}
