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
package com.dydabo.blackbox.usecase.medical.db;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
class PhoneNumber {

    private String ph = null;

    public PhoneNumber(String phoneNumber) {
        boolean valid = validatePhoneNumber(phoneNumber);
        if (valid) {
            this.ph = phoneNumber;
        }
    }

    @Override
    public String toString() {
        return "PhoneNumber{" + "ph=" + ph + '}';
    }

    private boolean validatePhoneNumber(String phoneNumber) {
        if (phoneNumber == null) {
            return false;
        }
        if (phoneNumber.trim().length() != 10) {
            return false;
        }

        return true;
    }

    public String getPh() {
        return ph;
    }

    public void setPh(String ph) {
        this.ph = ph;
    }

}
