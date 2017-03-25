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
public class Diagnosis implements BlackBoxable {

    private String dId = null;
    private String dNotes = null;

    public Diagnosis(String dId) {
        this.dId = dId;
    }

    public String getdId() {
        return dId;
    }

    public void setdId(String dId) {
        this.dId = dId;
    }

    public String getdNotes() {
        return dNotes;
    }

    public void setdNotes(String dNotes) {
        this.dNotes = dNotes;
    }

    @Override
    public String getBBRowKey() {
        return getdId();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "Diagnosis{" + "dId=" + dId + ", dNotes=" + dNotes + '}';
    }

}
