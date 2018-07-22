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

package com.dydabo.blackbox.blackbox.usecase.medical;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.redis.RedisBlackBoxImpl;
import com.dydabo.blackbox.usecase.company.SimpleUseCase;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author viswadas leher
 */
public class RedisTest extends SimpleUseCase {
    protected BlackBox instance;

    /**
     * @throws IOException
     */
    public RedisTest() throws IOException {
        super();
        if (utils.dbToTest.contains(BlackBoxFactory.Databases.REDIS)) {
            this.instance = new RedisBlackBoxImpl();
        }
    }

    @Test
    public void testInsert() {
        int testSize = 1000;

        if (this.instance != null) {
            insertionTest(testSize, this.instance);
        }
    }

    @Test
    public void testUpdate() {
        int testSize = 1000;

        if (this.instance != null) {
            updateTest(testSize, this.instance);
        }
    }

    @Test
    public void testDelete() {
        int testSize = 1000;
        if (this.instance != null) {
            deleteTest(testSize, this.instance);
        }
    }

    @Test
    public void testFetchByPartialKey() {
        int testSize = 200;
        if (this.instance != null) {
            fetchPartialKey(testSize, this.instance);
        }
    }

    @Test
    public void testSearchByName() {
        int testSize = 5;
        if (this.instance != null) {
            searchTestByName(testSize, this.instance);
        }
    }

    @Test
    public void testSearchMultipleTypes() {
        int testSize = 200;
        if (this.instance != null) {
            searchMultipleTypes(testSize, this.instance);
        }
    }

    @Test
    public void testSearchWithWildCards() {
        int testSize = 200;
        if (instance != null) {
            searchWithWildCards(testSize, instance);
        }
    }

    @Test
    public void testSearchWithDouble() {
        int testSize = 200;
        if (instance != null) {
            searchWithDouble(testSize, instance);
        }
    }

    @Test
    public void testRangeSearchDouble() {
        int testSize = 200;
        if (instance != null) {
            rangeSearchDouble(testSize, instance);
        }
    }
}
