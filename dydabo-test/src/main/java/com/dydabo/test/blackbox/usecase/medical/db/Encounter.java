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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * @author viswadas leher
 */
public class Encounter implements BlackBoxable {

    private String patientId = null;
    private String encounterId = null;
    private String patientNotes = null;
    private List<Procedure> procedures = new LinkedList<>();
    private List<Medication> medications = new LinkedList<>();
    private List<Diagnosis> diagnoses = new LinkedList<>();
    private List<Claim> claims = new LinkedList<>();

    // Selective Duplicate Data for easy access and search
    private String patientFirstName = null;
    private String patientLastName = null;

    private String medicationIds = "";
    private String diagnosisIds = "";

    // Complete Duplicate Data
    private Patient patient = null;

    /**
     *
     */
    public Encounter() {
    }

    /**
     * @param encounterId
     * @param patientId
     */
    public Encounter(String encounterId, String patientId) {
        this.encounterId = encounterId;
        this.patientId = patientId;
    }

    /**
     * @param encounterId
     * @param patientId
     * @param patientFirstName
     * @param patientLastName
     */
    public Encounter(String encounterId, String patientId, String patientFirstName, String patientLastName) {
        this(encounterId, patientId);
        this.patientFirstName = patientFirstName;
        this.patientLastName = patientLastName;
    }

    /**
     * @param encounterId
     * @param pat
     */
    public Encounter(String encounterId, Patient pat) {
        this(encounterId, pat.getPatientId(), pat.getFirstName(), pat.getLastName());
        this.patient = pat;
    }

    /**
     * @return
     */
    public String getPatientId() {
        return patientId;
    }

    /**
     * @param patientId
     */
    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    /**
     * @return
     */
    public String getEncounterId() {
        return encounterId;
    }

    /**
     * @param encounterId
     */
    public void setEncounterId(String encounterId) {
        this.encounterId = encounterId;
    }

    /**
     * @return
     */
    public String getPatientNotes() {
        return patientNotes;
    }

    /**
     * @param patientNotes
     */
    public void setPatientNotes(String patientNotes) {
        this.patientNotes = patientNotes;
    }

    /**
     * @return
     */
    public List<Procedure> getProcedures() {
        return procedures;
    }

    /**
     * @param procedures
     */
    public void setProcedures(List<Procedure> procedures) {
        this.procedures = procedures;
    }

    /**
     * @return
     */
    public List<Medication> getMedications() {
        return medications;
    }

    /**
     * @param medications
     */
    public void setMedications(List<Medication> medications) {
        this.medications = medications;
    }

    /**
     * @return
     */
    public List<Diagnosis> getDiagnoses() {
        return diagnoses;
    }

    /**
     * @param diagnoses
     */
    public void setDiagnoses(List<Diagnosis> diagnoses) {
        this.diagnoses = diagnoses;
    }

    /**
     * @return
     */
    public List<Claim> getClaims() {
        return claims;
    }

    /**
     * @param claims
     */
    public void setClaims(List<Claim> claims) {
        this.claims = claims;
    }

    /**
     * @return
     */
    public String getPatientFirstName() {
        return patientFirstName;
    }

    /**
     * @param patientFirstName
     */
    public void setPatientFirstName(String patientFirstName) {
        this.patientFirstName = patientFirstName;
    }

    /**
     * @return
     */
    public String getPatientLastName() {
        return patientLastName;
    }

    /**
     * @param patientLastName
     */
    public void setPatientLastName(String patientLastName) {
        this.patientLastName = patientLastName;
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
    public String getMedicationIds() {
        return medicationIds;
    }

    /**
     * @param medicationIds
     */
    public void setMedicationIds(String medicationIds) {
        this.medicationIds = medicationIds;
    }

    /**
     * @return
     */
    public String getDiagnosisIds() {
        return diagnosisIds;
    }

    /**
     * @param diagnosisIds
     */
    public void setDiagnosisIds(String diagnosisIds) {
        this.diagnosisIds = diagnosisIds;
    }

    @Override
    public List<Optional<Object>> getBBRowKeys() {
        return Arrays.asList(Optional.ofNullable(getPatientId()), Optional.ofNullable(getEncounterId()));
    }

    @Override
    public String toString() {
        return "PatientEncounter{" + "pId=" + patientId + ", eId=" + encounterId + ", pNotes=" + patientNotes + ", procs=" + procedures + ", meds=" + medications + ", diags=" + diagnoses + ", cls=" + claims + ", pFN=" + patientFirstName + ", pLN=" + patientLastName + ", patient=" + patient + '}';
    }

    /**
     * @param diagnosis
     */
    public void addDiagnosis(Diagnosis diagnosis) {
        getDiagnoses().add(diagnosis);
        setDiagnosisIds(getDiagnosisIds() + "," + diagnosis.getdId());
    }

    /**
     * @param medication
     */
    public void addMedication(Medication medication) {
        getMedications().add(medication);
        setMedicationIds(getMedicationIds() + "," + medication.getmId());
    }

    /**
     * @param claim
     */
    public void addClaim(Claim claim) {
        getClaims().add(claim);
    }
}
