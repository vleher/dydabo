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
package com.dydabo.blackbox.usecase.medical.db;

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;

import java.util.LinkedList;
import java.util.List;

/**
 * @author viswadas leher
 */
public class Encounter implements BlackBoxable {

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

    /**
     *
     */
    public Encounter() {
    }

    /**
     * @param eId
     * @param pId
     */
    public Encounter(String eId, String pId) {
        this.eId = eId;
        this.pId = pId;
    }

    /**
     * @param eId
     * @param pId
     * @param pFN
     * @param pLN
     */
    public Encounter(String eId, String pId, String pFN, String pLN) {
        this(eId, pId);
        this.pFN = pFN;
        this.pLN = pLN;
    }

    /**
     * @param eId
     * @param pat
     */
    public Encounter(String eId, Patient pat) {
        this(eId, pat.getpId(), pat.getfN(), pat.getlN());
        this.patient = pat;
    }

    /**
     * @return
     */
    public String getpId() {
        return pId;
    }

    /**
     * @param pId
     */
    public void setpId(String pId) {
        this.pId = pId;
    }

    /**
     * @return
     */
    public String geteId() {
        return eId;
    }

    /**
     * @param eId
     */
    public void seteId(String eId) {
        this.eId = eId;
    }

    /**
     * @return
     */
    public String getpNotes() {
        return pNotes;
    }

    /**
     * @param pNotes
     */
    public void setpNotes(String pNotes) {
        this.pNotes = pNotes;
    }

    /**
     * @return
     */
    public List<Procedure> getProcs() {
        return procs;
    }

    /**
     * @param procs
     */
    public void setProcs(List<Procedure> procs) {
        this.procs = procs;
    }

    /**
     * @return
     */
    public List<Medication> getMeds() {
        return meds;
    }

    /**
     * @param meds
     */
    public void setMeds(List<Medication> meds) {
        this.meds = meds;
    }

    /**
     * @return
     */
    public List<Diagnosis> getDiags() {
        return diags;
    }

    /**
     * @param diags
     */
    public void setDiags(List<Diagnosis> diags) {
        this.diags = diags;
    }

    /**
     * @return
     */
    public List<Claim> getCls() {
        return cls;
    }

    /**
     * @param cls
     */
    public void setCls(List<Claim> cls) {
        this.cls = cls;
    }

    /**
     * @return
     */
    public String getpFN() {
        return pFN;
    }

    /**
     * @param pFN
     */
    public void setpFN(String pFN) {
        this.pFN = pFN;
    }

    /**
     * @return
     */
    public String getpLN() {
        return pLN;
    }

    /**
     * @param pLN
     */
    public void setpLN(String pLN) {
        this.pLN = pLN;
    }

    /**
     * @return
     */
    public Patient getPatient() {
        return patient;
    }

    /**
     * @param patient
     */
    public void setPatient(Patient patient) {
        this.patient = patient;
    }

    /**
     * @return
     */
    public String getMedIds() {
        return medIds;
    }

    /**
     * @param medIds
     */
    public void setMedIds(String medIds) {
        this.medIds = medIds;
    }

    /**
     * @return
     */
    public String getDiagIds() {
        return diagIds;
    }

    /**
     * @param diagIds
     */
    public void setDiagIds(String diagIds) {
        this.diagIds = diagIds;
    }

    @Override
    public String getBBRowKey() {
        return getpId() + ":" + geteId();
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
        return "PatientEncounter{" + "pId=" + pId + ", eId=" + eId + ", pNotes=" + pNotes + ", procs=" + procs + ", meds=" + meds + ", diags=" + diags + ", cls=" + cls + ", pFN=" + pFN + ", pLN=" + pLN + ", patient=" + patient + '}';
    }

    /**
     * @param diagnosis
     */
    public void addDiagnosis(Diagnosis diagnosis) {
        getDiags().add(diagnosis);
        setDiagIds(getDiagIds() + "," + diagnosis.getdId());
    }

    /**
     * @param medication
     */
    public void addMedication(Medication medication) {
        getMeds().add(medication);
        setMedIds(getMedIds() + "," + medication.getmId());
    }

    /**
     * @param claim
     */
    public void addClaim(Claim claim) {
        getCls().add(claim);
    }

}
