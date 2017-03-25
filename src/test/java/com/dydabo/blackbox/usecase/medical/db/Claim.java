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
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class Claim implements BlackBoxable {

    private String cId = null;
    private List<ClaimDetails> cDets = null;
    private List<ClaimCharges> cCharges = null;

    public Claim(String cId) {
        cDets = new LinkedList<>();
        cCharges = new LinkedList<>();
        this.cId = cId;
    }

    public String getcId() {
        return cId;
    }

    public void setcId(String cId) {
        this.cId = cId;
    }

    public List<ClaimDetails> getcDets() {
        return cDets;
    }

    public void setcDets(List<ClaimDetails> cDets) {
        this.cDets = cDets;
    }

    public List<ClaimCharges> getcCharges() {
        return cCharges;
    }

    public void setcCharges(List<ClaimCharges> cCharges) {
        this.cCharges = cCharges;
    }

    @Override
    public String getBBRowKey() {
        return getcId();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "Claim{" + "cId=" + cId + ", cDets=" + cDets + ", cCharges=" + cCharges + '}';
    }

}
