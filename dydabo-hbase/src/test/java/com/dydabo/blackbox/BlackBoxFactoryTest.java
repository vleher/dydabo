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

import com.dydabo.blackbox.hbase.HBaseBlackBoxImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.logging.Logger;

/**
 * @author viswadas leher
 */
public class BlackBoxFactoryTest {

    private final Logger logger = Logger.getLogger(BlackBoxFactoryTest.class.getName());


    /**
     * Test of getDatabase method, of class BlackBoxFactory.
     */
    @Test
    public void testGetDatabase() {
        // Test Hbase
        BlackBox<BlackBoxable> result = new HBaseBlackBoxImpl<>();
        Assertions.assertTrue(result instanceof HBaseBlackBoxImpl);
    }

    /**
     * Test of getHBaseDatabase method, of class BlackBoxFactory.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetHBaseDatabase() throws Exception {
        Configuration config = HBaseConfiguration.create();
        BlackBox<BlackBoxable> result = new HBaseBlackBoxImpl<>(config);
        Assertions.assertTrue(result instanceof HBaseBlackBoxImpl);
    }

    /**
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    @Test
    public void testConstructorIsPrivate()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<BlackBoxFactory> constructor = BlackBoxFactory.class.getDeclaredConstructor();
        Assertions.assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }
}
