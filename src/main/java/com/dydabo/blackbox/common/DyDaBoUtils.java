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
package com.dydabo.blackbox.common;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class DyDaBoUtils {

    public static final String EMPTY_ARRAY = "[]";
    public static final String EMPTY_MAP = "{}";

    public static boolean isBlankOrNull(String... str) {
        for (String s : str) {
            if (s == null || s.trim().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public static boolean isValidRegex(String regexValue) {

        if (isBlankOrNull(regexValue)) {
            return false;
        }

        if (EMPTY_ARRAY.equals(regexValue)) {
            return false;
        }

        if (EMPTY_MAP.equals(regexValue)) {
            return false;
        }

        return true;
    }

}
