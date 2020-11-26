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

package com.dydabo.blackbox.cassandra;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.usecase.company.SimpleUseCase;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author viswadas leher
 */
public class CassandraTest extends SimpleUseCase {

    protected BlackBox instance;

    /**
     * @throws IOException
     */
    public CassandraTest() throws IOException {
        super();
        if (utils.dbToTest.contains(BlackBoxFactory.Databases.CASSANDRA)) {
            final CassandraConnectionManager cassandraConnectionManager = new CassandraConnectionManager();
            cassandraConnectionManager.setAddress("127.0.0.1");
            instance = new CassandraBlackBox(cassandraConnectionManager);
        }
    }

    @Test
    public void testInsert() {
        int testSize = 10;

        if (instance != null) {
            insertionTest(testSize, instance);
        }
    }

    @Test
    public void testUpdate() {
        int testSize = 10;

        if (instance != null) {
            updateTest(testSize, instance);
        }
    }

    @Test
    public void testDelete() {
        int testSize = 10;
        if (instance != null) {
            deleteTest(testSize, instance);
        }
    }

    @Test
    public void testFetchByPartialKey() {
        int testSize = 2;
        if (instance != null) {
            fetchPartialKey(testSize, instance);
        }
    }

    @Test
    public void testSearchByName() {
        int testSize = 5;
        if (instance != null) {
            searchTestByName(testSize, instance);
        }
    }

    @Test
    public void testSearchMultipleTypes() {
        int testSize = 2;
        if (instance != null) {
            searchMultipleTypes(testSize, instance);
        }
    }

    @Test
    public void testSearchWithWildCards() {
        int testSize = 2;
        if (instance != null) {
            searchWithWildCards(testSize, instance);
        }
    }

    @Test
    public void testSearchWithDouble() {
        int testSize = 2;
        if (instance != null) {
            searchWithDouble(testSize, instance);
        }
    }

    @Test
    public void testRangeSearchDouble() {
        int testSize = 2;
        if (instance != null) {
            rangeSearchDouble(testSize, instance);
        }
    }

}
