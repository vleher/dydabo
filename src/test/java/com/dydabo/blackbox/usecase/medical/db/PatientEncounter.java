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
public class PatientEncounter implements BlackBoxable {

    private String pId = null;
    private String eId = null;
    private String pNotes = null;
    private List<Procedure> procs = new LinkedList<>();
    private List<Medication> meds = new LinkedList<>();
    private List<Diagnosis> diags = new LinkedList<>();
    private List<Claim> cls = new LinkedList<>();

    // Selective Duplicate Data for easy access and search
    private String pFN = null;
    private String pLN = null;

    private String medIds = "";
    private String diagIds = "";

    // Complete Duplicate Data
    private Patient patient = null;

    public PatientEncounter() {
    }

    public PatientEncounter(String eId, String pId) {
        this.eId = eId;
        this.pId = pId;
    }

    public PatientEncounter(String eId, String pId, String pFN, String pLN) {
        this(eId, pId);
        this.pFN = pFN;
        this.pLN = pLN;
    }

    public PatientEncounter(String eId, Patient pat) {
        this(eId, pat.getpId());
        this.patient = pat;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public String geteId() {
        return eId;
    }

    public void seteId(String eId) {
        this.eId = eId;
    }

    public String getpNotes() {
        return pNotes;
    }

    public void setpNotes(String pNotes) {
        this.pNotes = pNotes;
    }

    public List<Procedure> getProcs() {
        return procs;
    }

    public void setProcs(List<Procedure> procs) {
        this.procs = procs;
    }

    public List<Medication> getMeds() {
        return meds;
    }

    public void setMeds(List<Medication> meds) {
        this.meds = meds;
    }

    public List<Diagnosis> getDiags() {
        return diags;
    }

    public void setDiags(List<Diagnosis> diags) {
        this.diags = diags;
    }

    public List<Claim> getCls() {
        return cls;
    }

    public String getpFN() {
        return pFN;
    }

    public void setpFN(String pFN) {
        this.pFN = pFN;
    }

    public String getpLN() {
        return pLN;
    }

    public void setpLN(String pLN) {
        this.pLN = pLN;
    }

    public Patient getPatient() {
        return patient;
    }

    public void setPatient(Patient patient) {
        this.patient = patient;
    }

    public void setCls(List<Claim> cls) {
        this.cls = cls;
    }

    public String getMedIds() {
        return medIds;
    }

    public void setMedIds(String medIds) {
        this.medIds = medIds;
    }

    public String getDiagIds() {
        return diagIds;
    }

    public void setDiagIds(String diagIds) {
        this.diagIds = diagIds;
    }

    @Override
    public String getBBRowKey() {
        return getpId() + ":" + geteId();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "PatientEncounter{" + "pId=" + pId + ", eId=" + eId + ", pNotes=" + pNotes + ", procs=" + procs + ", meds=" + meds + ", diags=" + diags + ", cls=" + cls + ", pFN=" + pFN + ", pLN=" + pLN + ", patient=" + patient + '}';
    }

    public void addDiagnosis(Diagnosis diagnosis) {
        getDiags().add(diagnosis);
        setDiagIds(getDiagIds() + "," + diagnosis.getdId());
    }

    public void addMedication(Medication medication) {
        getMeds().add(medication);
        setMedIds(getMedIds() + "," + medication.getmId());
    }

    public void addClaim(Claim claim) {
        getCls().add(claim);
    }

}
