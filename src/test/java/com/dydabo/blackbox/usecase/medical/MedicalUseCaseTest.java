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
package com.dydabo.blackbox.usecase.medical;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.usecase.medical.db.Claim;
import com.dydabo.blackbox.usecase.medical.db.ClaimCharges;
import com.dydabo.blackbox.usecase.medical.db.ClaimDetails;
import com.dydabo.blackbox.usecase.medical.db.Diagnosis;
import com.dydabo.blackbox.usecase.medical.db.Medication;
import com.dydabo.blackbox.usecase.medical.db.Patient;
import com.dydabo.blackbox.usecase.medical.db.PatientEncounter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class MedicalUseCaseTest {

    private static BlackBox blackBox;
    public List<String> Diagnosis = Arrays.asList("Diabetes", "High Blood Pressure", "Low Blood Pressure", "Polio", "Fever",
            "Common Cold", "Allergy");
    public List<String> FirstNames = Arrays.asList("David", "Peter", "Tom", "Dick", "Harry", "John", "Bill", "Adele", "Britney",
            "Mariah", "Tina", "Diana", "Dionne", "Cyndi", "Kim", "Lindsey", "Shiela", "Bette");
    public List<String> LastNames = Arrays.asList("Johnson", "Becker", "Smith", "Gates", "King", "Spears", "Perry", "Carey",
            "Gomez", "Lopez", "Turner", "Ross", "Warwick", "Lauper", "Carnes", "Midler", "Jackson", "Hayes");

    public List<String> Meds = Arrays.asList("Acetylmethadol", "Benzethidine", "Difenoxin", "Furethidine", "Phenoperidine");

    int knownPatientId = 123456;
    Random random = new Random();

    public MedicalUseCaseTest() throws BlackBoxException, IOException {
        blackBox = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
        // Pre-populate with some dynamic data.
        generatePatients(2);
        generateEncounters(3);
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    private void generateEncounters(int count) throws BlackBoxException {
        Patient p = new Patient();
        List<Patient> pList = blackBox.search(Arrays.asList(p));
        List<PatientEncounter> encounters = new ArrayList<>();
        for (int j = 0; j < count; j++) {
            int id = random.nextInt();
            final Patient currentPatient = pList.get(Math.abs(id % pList.size()));
            PatientEncounter enc = new PatientEncounter(id + "E", currentPatient);

            enc.setPatient(currentPatient);
            enc.setpFN(currentPatient.getfN());
            enc.setpLN(currentPatient.getlN());

            // Add random # of diagnosis
            int dCount = random.nextInt(2);
            for (int i = 0; i < dCount; i++) {
                final Diagnosis diagnosis = new Diagnosis(Diagnosis.get(random.nextInt(10000) % Diagnosis.size()));
                if (blackBox.update(diagnosis)) {
                    enc.addDiagnosis(diagnosis);
                }
            }
            // Add random Medications
            dCount = random.nextInt(2);
            for (int i = 0; i < dCount; i++) {
                final Medication medication = new Medication(Meds.get(random.nextInt(10000) % Meds.size()));
                medication.setmDose(random.nextInt(8));
                if (blackBox.update(medication)) {
                    enc.addMedication(medication);
                }
            }

            // add random # of claims
            dCount = random.nextInt(2);
            for (int i = 0; i < dCount; i++) {
                final Random random = new Random();
                final Claim claim = new Claim(random.nextInt() + "CL");

                //  generate random # of details and charges
                int cdCount = random.nextInt(5);
                for (int k = 0; k < cdCount; k++) {
                    ClaimDetails cDet = new ClaimDetails(random.nextInt() + "CD");
                    if (blackBox.update(cDet)) {
                        claim.getcDets().add(cDet);
                    }
                }
                cdCount = random.nextInt(5);
                for (int k = 0; k < cdCount; k++) {
                    ClaimCharges cc = new ClaimCharges(random.nextInt() + "CC", random.nextInt(10000) * 1.1);
                    if (blackBox.update(cc)) {
                        claim.getcCharges().add(cc);
                    }
                }

                if (blackBox.update(claim)) {
                    enc.addClaim(claim);
                }
            }

            encounters.add(enc);
        }
        blackBox.update(encounters);

    }

    private void generatePatients(int count) throws BlackBoxException {
        List<Patient> patientList = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            int id = random.nextInt();
            Patient patient = new Patient(id + "P", FirstNames.get(Math.abs(id % FirstNames.size())),
                    LastNames.get(Math.abs(random.nextInt() % LastNames.size())));
            patient.initData();
            patientList.add(patient);
        }
        // update the table
        blackBox.update(patientList);
        // Create some patients with specific ids so that we can query them

        Patient p = new Patient(knownPatientId + "P", FirstNames.get(Math.abs(knownPatientId % FirstNames.size())),
                LastNames.get(Math.abs(knownPatientId % LastNames.size())));
        p.initData();
        blackBox.update(Arrays.asList(p));
    }

    @BeforeMethod
    public void setUpMethod() throws Exception {
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

    @Test
    public void testAllPatientEncounters() throws BlackBoxException {
        // All Encounters
        PatientEncounter pe = new PatientEncounter();
        List<PatientEncounter> peL1 = blackBox.search(Arrays.asList(pe));
        Assert.assertTrue(peL1.size() > 0);
    }

    @Test
    public void testAllPatients() throws BlackBoxException {
        // All Patients
        Patient p = new Patient();
        List<Patient> pList1 = blackBox.search(Arrays.asList(p));
        Assert.assertTrue(pList1.size() > 0);
    }

    @Test
    public void testPatientByName() throws BlackBoxException {
        // All Patients with a specific first name (column value search)
        Patient p = new Patient();
        p.setfN("Tina");
        List<Patient> pList3 = blackBox.search(Arrays.asList(p));
        for (Patient pat : pList3) {
            Assert.assertEquals(pat.getfN(), "Tina");
        }
        // All Patients with specific first name (row key search)
        String fNameKey = ".*:Cyndi:.*";
        List<Patient> pList4 = blackBox.fetchByPartialKey(Arrays.asList(fNameKey), p);
        for (Patient pat : pList4) {
            Assert.assertEquals(pat.getfN(), "Cyndi");
        }

    }

    @Test
    public void testPatientEncountersById() throws BlackBoxException {
        PatientEncounter pe = new PatientEncounter();
        // All encounters by patient Id (row key)
        List<String> query = Arrays.asList(knownPatientId + "P:.*");
        List<PatientEncounter> peL2 = blackBox.fetchByPartialKey(query, pe);
        for (PatientEncounter penc : peL2) {
            Assert.assertEquals(penc.getpId(), knownPatientId + "P");
        }

    }

    @Test
    public void testPatientEncountersByMedId() throws BlackBoxException {

        // All encounters with a medication id
        List<Medication> med1 = blackBox.search(new Medication());
        if (med1.size() > 0) {
            String randMedId = med1.get(Math.abs(random.nextInt() % med1.size())).getmId();
            PatientEncounter pe2 = new PatientEncounter();
            pe2.setMedIds(".*" + randMedId + ".*");

            List<PatientEncounter> encs = blackBox.search(pe2);
            for (PatientEncounter enc : encs) {
                Assert.assertTrue(enc.getMedIds().contains(randMedId));
                Assert.assertTrue(!enc.getPatient().getfN().isEmpty());
            }
        }
    }

    @Test
    public void testPatientEncountersByMedIdAndPFName() throws BlackBoxException {
        // get a patient
        List<Patient> allPatients = blackBox.search(new Patient());
        Assert.assertTrue(allPatients.size() > 0);
        // random patient first name
        String firstName = allPatients.get(Math.abs(random.nextInt() % allPatients.size())).getfN();
        // get a medication name
        List<Medication> allMeds = blackBox.search(new Medication());
        // random medication name
        String medId = allMeds.get(Math.abs(random.nextInt() % allMeds.size())).getmId();
        // search encounters
        PatientEncounter pe = new PatientEncounter();
        pe.setpFN(firstName);
        pe.setMedIds(".*" + medId + ".*");
        List<PatientEncounter> results = blackBox.search(pe);

        for (PatientEncounter r : results) {
            Assert.assertEquals(r.getPatient().getfN(), firstName);
            Assert.assertEquals(r.getpFN(), firstName);
            Assert.assertTrue(r.getMedIds().contains(medId));
        }
    }

    @Test
    public void testAllPatientsWithDiagnosis() throws BlackBoxException {

        String diagId = Diagnosis.get(Math.abs(random.nextInt() % Diagnosis.size()));

        PatientEncounter pe = new PatientEncounter();
        pe.setDiagIds(".*" + diagId + ".*");

        List<PatientEncounter> peList = blackBox.search(pe);
        for (PatientEncounter enc : peList) {
            Assert.assertTrue(enc.getDiagIds().contains(diagId));
        }

        PatientEncounter pe1 = new PatientEncounter();
        List<Diagnosis> diags = new ArrayList<>();
        Diagnosis d = new Diagnosis(diagId);
        diags.add(d);

        pe1.setDiags(diags);

        List<PatientEncounter> peList1 = blackBox.search(pe1);
        for (PatientEncounter enc : peList) {
            Assert.assertTrue(enc.getDiagIds().contains(diagId));
        }

    }

    @Test
    public void testPatientEncountersByName() throws BlackBoxException {

        // get some data that exists
        List<PatientEncounter> allPEs = blackBox.search(new PatientEncounter());
        int rId = Math.abs(random.nextInt() % allPEs.size());
        String firstName = allPEs.get(rId).getpFN();
        String lastName = allPEs.get(rId).getpLN();

        PatientEncounter pe1 = new PatientEncounter();
        // All encounters for patient names
        List<Patient> p2 = blackBox.fetchByPartialKey(".*:" + firstName + ":" + lastName, new Patient());
        List<PatientEncounter> peL3 = new ArrayList<>();
        for (Patient patient : p2) {
            List<PatientEncounter> temp = blackBox.fetchByPartialKey(patient.getpId() + ":.*", pe1);
            peL3.addAll(temp);
        }

        Assert.assertTrue(peL3.size() > 0);
        for (PatientEncounter pe : peL3) {
            Assert.assertEquals(pe.getpFN(), firstName);
            Assert.assertEquals(pe.getpLN(), lastName);
            Assert.assertEquals(pe.getPatient().getfN(), firstName);
            Assert.assertEquals(pe.getPatient().getlN(), lastName);
        }

        PatientEncounter pe2 = new PatientEncounter(null, null, firstName, lastName);
        List<PatientEncounter> peL4 = blackBox.search(pe2);
        Assert.assertTrue(peL4.size() > 0);
        Assert.assertEquals(peL4.size(), peL3.size());

        for (PatientEncounter pe : peL4) {
            Assert.assertEquals(pe.getpFN(), firstName);
            Assert.assertEquals(pe.getpLN(), lastName);
            Assert.assertEquals(pe.getPatient().getfN(), firstName);
            Assert.assertEquals(pe.getPatient().getlN(), lastName);
        }

        PatientEncounter pe3 = new PatientEncounter(null, new Patient(null, firstName, null));
        List<PatientEncounter> peL5 = blackBox.search(pe3);
        Assert.assertTrue(peL5.size() > 0);
        for (PatientEncounter pe : peL5) {
            Assert.assertEquals(pe.getPatient().getfN(), firstName);
        }
    }

    @Test
    public void testPatientWithId() throws BlackBoxException {
        Patient p = new Patient();
        // All Patients with patient id (row key search)
        String pIDKey = "123456P:.*";
        List<Patient> pList2 = blackBox.fetchByPartialKey(Arrays.asList(pIDKey), p);
        for (Patient pat : pList2) {
            Assert.assertEquals(pat.getpId(), "123456P");
        }
    }

    @Test
    public void testPatientsWithFirstAndLastName() throws BlackBoxException {

        Patient p = new Patient();
        // All Patients with first name 'Diana' OR last 'Turner'
        List<String> queryKeys = Arrays.asList(".*:Diana:.*", ".*:.*:Turner");
        List<Patient> pList5 = blackBox.fetchByPartialKey(queryKeys, p);
        for (Patient pat : pList5) {
            boolean result = "Diana".equals(pat.getfN()) || "Turner".equals(pat.getlN());
            Assert.assertTrue(result);
        }
    }

}
