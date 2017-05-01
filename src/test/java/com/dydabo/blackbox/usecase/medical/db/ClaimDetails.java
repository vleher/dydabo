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
package com.dydabo.blackbox.usecase.medical.db;

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;

/**
 * @author viswadas leher <vleher@gmail.com>
 */
public class ClaimDetails implements BlackBoxable {

    private String cdId = null;
    private String cdNotes = null;

    /**
     * @param cdId
     */
    public ClaimDetails(String cdId) {
        this.cdId = cdId;
    }

    /**
     * @return
     */
    public String getCdId() {
        return cdId;
    }

    /**
     * @param cdId
     */
    public void setCdId(String cdId) {
        this.cdId = cdId;
    }

    /**
     * @return
     */
    public String getCdNotes() {
        return cdNotes;
    }

    /**
     * @param cdNotes
     */
    public void setCdNotes(String cdNotes) {
        this.cdNotes = cdNotes;
    }

    @Override
    public String getBBRowKey() {
        return getCdId();
    }

    /**
     * @return
     */
    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "ClaimDetails{" + "cdId=" + cdId + ", cdNotes=" + cdNotes + '}';
    }

}
