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

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class ClaimDetails implements BlackBoxable {

    private String cdId = null;
    private String cdNotes = null;

    public ClaimDetails(String cdId) {
        this.cdId = cdId;
    }

    public String getCdId() {
        return cdId;
    }

    public void setCdId(String cdId) {
        this.cdId = cdId;
    }

    public String getCdNotes() {
        return cdNotes;
    }

    public void setCdNotes(String cdNotes) {
        this.cdNotes = cdNotes;
    }

    @Override
    public String getBBRowKey() {
        return getCdId();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "ClaimDetails{" + "cdId=" + cdId + ", cdNotes=" + cdNotes + '}';
    }

}
