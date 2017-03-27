/*******************************************************************************
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
 *******************************************************************************/
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
