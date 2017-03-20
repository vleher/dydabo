/*
 * Copyright (C) 2017 viswadas leher <vleher@gmail.com>
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

import com.dydabo.blackbox.hbase.HBaseJsonImpl;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class BlackBoxFactory {

    /**
     *
     */
    public final static String HBASE = "hbase";

    private BlackBoxFactory() {
        // No instance of this factory class
    }

    /**
     *
     * @param databaseType
     * @param conn
     * @return
     */
    public static BlackBox getDatabase(String databaseType) throws IOException {
        switch (databaseType) {
            case HBASE:
                return new HBaseJsonImpl();
            default:
                return null;
        }
    }

    public static BlackBox getHBaseDatabase(Configuration config) throws IOException {
        return new HBaseJsonImpl(config);
    }
}
