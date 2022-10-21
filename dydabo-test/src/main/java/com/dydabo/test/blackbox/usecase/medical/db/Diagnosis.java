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
package com.dydabo.test.blackbox.usecase.medical.db;

import com.dydabo.blackbox.BlackBoxable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * @author viswadas leher
 */
public class Diagnosis implements BlackBoxable {

    private String dId = null;
    private String dNotes = null;

    /**
     * @param dId
     */
    public Diagnosis(String dId) {
        this.dId = dId;
    }

    /**
     * @return
     */
    public String getdId() {
        return dId;
    }

    /**
     * @param dId
     */
    public void setdId(String dId) {
        this.dId = dId;
    }

    /**
     * @return
     */
    public String getdNotes() {
        return dNotes;
    }

    /**
     * @param dNotes
     */
    public void setdNotes(String dNotes) {
        this.dNotes = dNotes;
    }

    @Override
    public List<Optional<Object>> getBBRowKeys() {
        return Arrays.asList(Optional.ofNullable(dId));
    }

    @Override
    public String toString() {
        return "Diagnosis{" + "dId=" + dId + ", dNotes=" + dNotes + '}';
    }
}
