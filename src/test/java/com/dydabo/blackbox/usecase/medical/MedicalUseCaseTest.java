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
package com.dydabo.blackbox.usecase.medical;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.usecase.medical.db.Claim;
import com.dydabo.blackbox.usecase.medical.db.ClaimCharges;
import com.dydabo.blackbox.usecase.medical.db.Encounter;
import com.dydabo.blackbox.usecase.medical.db.Medication;
import com.dydabo.blackbox.usecase.medical.db.Patient;
import com.dydabo.blackbox.utils.DyDaBoTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

/**
 * @author viswadas leher
 */
public abstract class MedicalUseCaseTest {

    /**
     *
     */
    public static final List<String> Diagnosis = Arrays.asList("Diabetes", "High Blood Pressure", "Low Blood Pressure", "Polio", "Fever",
            "Common Cold", "Allergy");
    /**
     *
     */
    public static final List<String> Meds = Arrays.asList("Acetylmethadol", "Benzethidine", "Difenoxin", "Furethidine", "Phenoperidine");
    protected static final Logger logger = Logger.getLogger(MedicalUseCaseTest.class.getName());
    /**
     *
     */
    public static List<String> FirstNames = Arrays.asList("David", "Peter", "Tom", "Dick", "Harry", "John", "Bill", "Adele", "Britney",
            "Mariah", "Tina", "Diana", "Dionne", "Cyndi", "Kim", "Lindsey", "Shiela", "Bette");
    /**
     *
     */
    public static List<String> LastNames = Arrays.asList("Johnson", "Becker", "Smith", "Gates", "King", "Spears", "Perry", "Carey",
            "Gomez", "Lopez", "Turner", "Ross", "Warwick", "Lauper", "Carnes", "Midler", "Jackson", "Hayes");
    final int knownPatientId = 123456;
    final Random random = new Random();
    protected final DyDaBoTestUtils utils = new DyDaBoTestUtils();
    protected BlackBox instance;

    /**
     * @throws BlackBoxException
     * @throws IOException
     */
    public MedicalUseCaseTest() throws BlackBoxException, IOException {

    }


    /**
     * @throws BlackBoxException
     */
    @Test
    public void testPatientWithId() throws BlackBoxException {
        Patient p = new Patient();
        // All Patients with patient id (row key search)
        String pIDKey = "123456P:.*";
        List<Patient> pList2 = instance.fetchByPartialKey(pIDKey, p);
        for (Patient pat : pList2) {
            Assert.assertEquals(pat.getpId(), "123456P");
        }
    }

    @Test
    public void testAllPatientEncounters() throws BlackBoxException {
        // All Encounters
        Encounter pe = new Encounter();
        List<Encounter> peL1 = instance.search(Collections.singletonList(pe));
        Assert.assertTrue(peL1.size() > 0, "" + peL1.size());

        List<Encounter> peL2 = instance.search(pe, 7);
        Assert.assertTrue((peL2.size() <= 7), "" + peL2.size());

        List<Encounter> peL3 = instance.search(pe, 23);
        Assert.assertTrue(peL3.size() <= 23, "" + peL3.size());
    }

    @Test
    public void testAllPatients() throws BlackBoxException {
        // All Patients
        Patient p = new Patient();
        List<Patient> pList1 = instance.search(Collections.singletonList(p));
        Assert.assertTrue(pList1.size() > 0);
    }

    @Test
    public void testFetchQueryPerformance() throws BlackBoxException {
        // get some data that exists
        List<Encounter> allPEs = instance.search(new Encounter());
        int rId = Math.abs(random.nextInt() % allPEs.size());
        logger.info("rId :" + rId + ":" + allPEs.size());
        String firstName = allPEs.get(rId).getpFN();
        String lastName = allPEs.get(rId).getpLN();

        long startTime = System.nanoTime();
        List<Patient> p4 = instance.fetchByPartialKey(".*:" + firstName + ":" + lastName, new Patient());
        long endTime = System.nanoTime();
        logger.info("1 :" + p4.size() + ":" + (endTime - startTime));

        startTime = System.nanoTime();
        List<Patient> p5 = instance.fetchByPartialKey(".*:" + ".*:" + lastName, new Patient());
        endTime = System.nanoTime();
        logger.info("2 :" + p5.size() + ":" + (endTime - startTime));

        startTime = System.nanoTime();
        List<Patient> p6 = instance.fetchByPartialKey(".*:" + firstName + ":.*", new Patient());
        endTime = System.nanoTime();
        logger.info("3 :" + p6.size() + ":" + (endTime - startTime));
    }

    @Test
    public void testPatientByName() throws BlackBoxException {
        // All Patients with a specific first name (column value search)
        Patient p = new Patient();
        p.setfN("Tina");
        List<Patient> pList3 = instance.search(Collections.singletonList(p));
        for (Patient pat : pList3) {
            Assert.assertEquals(pat.getfN(), "Tina");
        }

        // All Patients with specific first name (row key search)
        String fNameKey = ".*:Cyndi:.*";
        List<Patient> pList4 = instance.fetchByPartialKey(Collections.singletonList(fNameKey), p);
        for (Patient pat : pList4) {
            Assert.assertEquals(pat.getfN(), "Cyndi");
        }

    }

    @Test
    public void testPatientEncountersById() throws BlackBoxException {
        Encounter pe = new Encounter();
        // All encounters by patient Id (row key)
        List<String> query = Collections.singletonList(knownPatientId + "P:.*");
        List<Encounter> peL2 = instance.fetchByPartialKey(query, pe);
        for (Encounter penc : peL2) {
            Assert.assertEquals(penc.getpId(), knownPatientId + "P");
        }

    }

    @Test
    public void testPatientEncountersByMedId() throws BlackBoxException {
        // All encounters with a medication id
        List<Medication> med1 = instance.search(new Medication());
        if (med1.size() > 0) {
            String randMedId = med1.get(Math.abs(random.nextInt() % med1.size())).getmId();
            Encounter pe2 = new Encounter();
            pe2.setMedIds(".*" + randMedId + ".*");

            List<Encounter> encs = instance.search(pe2);
            for (Encounter enc : encs) {
                Assert.assertTrue(enc.getMedIds().contains(randMedId), enc.getMedIds());
                if (enc.getPatient() != null) {
                    Assert.assertTrue(!enc.getPatient().getfN().isEmpty(), enc.getPatient().toString());
                }
            }
        }
    }

    @Test
    public void testPatientEncountersByMedIdAndPFName() throws BlackBoxException {
        // get a patient
        List<Patient> allPatients = instance.search(new Patient());
        Assert.assertTrue(allPatients.size() > 0);

        // random patient first name
        String firstName = allPatients.get(Math.abs(random.nextInt() % allPatients.size())).getfN();
        // get a medication name
        List<Medication> allMeds = instance.search(new Medication());
        // random medication name
        String medId = allMeds.get(Math.abs(random.nextInt() % allMeds.size())).getmId();
        // search encounters
        Encounter pe = new Encounter();
        pe.setpFN(firstName);
        pe.setMedIds(".*" + medId + ".*");
        List<Encounter> results = instance.search(pe);

        for (Encounter r : results) {
            if (r.getPatient() != null) {
                Assert.assertEquals(r.getPatient().getfN(), firstName, r.getPatient().toString());
            }
            Assert.assertEquals(r.getpFN(), firstName, r.toString());
            Assert.assertTrue(r.getMedIds().contains(medId), r.getMedIds());
        }
    }

    @Test
    public void testAllPatientsWithDiagnosis() throws BlackBoxException {

        String diagId = Diagnosis.get(Math.abs(random.nextInt() % Diagnosis.size()));

        Encounter pe = new Encounter();
        pe.setDiagIds(".*" + diagId + ".*");

        List<Encounter> peList = instance.search(pe);
        for (Encounter enc : peList) {
            Assert.assertTrue(enc.getDiagIds().contains(diagId));
        }

    }

    @Test
    public void testPatientEncountersByName() throws BlackBoxException {

        // get some data that exists
        List<Encounter> allPEs = instance.search(new Encounter());
        int rId = Math.abs(random.nextInt() % allPEs.size());
        String firstName = allPEs.get(rId).getpFN();
        String lastName = allPEs.get(rId).getpLN();

        logger.info("FN :" + firstName + " LN:" + lastName + " => " + allPEs.get(rId));

        Encounter pe1 = new Encounter();
        // All encounters for patient names
        List<Patient> p2 = instance.fetchByPartialKey(".*:" + firstName + ":" + lastName, new Patient());
        Assert.assertTrue(p2.size() > 0, "Size :" + p2.size());
        List<Encounter> peL3 = new ArrayList<>();
        for (Patient patient : p2) {
            List<Encounter> temp = instance.fetchByPartialKey(".*" + patient.getpId() + ":.*", pe1);
            peL3.addAll(temp);
        }

        Assert.assertTrue(peL3.size() > 0, "Size:" + peL3.size());
        for (Encounter pe : peL3) {
            logger.info(" ENc " + pe);
            Assert.assertEquals(pe.getpFN(), firstName);
            Assert.assertEquals(pe.getpLN(), lastName);
            if (pe.getPatient() != null) {
                Assert.assertEquals(pe.getPatient().getfN(), firstName, pe.getPatient().toString());
                Assert.assertEquals(pe.getPatient().getlN(), lastName, pe.getPatient().toString());
            }
        }

        Encounter pe2 = new Encounter(null, null, firstName, lastName);
        List<Encounter> peL4 = instance.search(pe2);
        Assert.assertTrue(peL4.size() > 0);
        Assert.assertEquals(peL4.size(), peL3.size());

        for (Encounter pe : peL4) {
            Assert.assertEquals(pe.getpFN(), firstName);
            Assert.assertEquals(pe.getpLN(), lastName);
            if (pe.getPatient() != null) {
                Assert.assertEquals(pe.getPatient().getfN(), firstName);
                Assert.assertEquals(pe.getPatient().getlN(), lastName);
            }
        }

        Encounter pe3 = new Encounter(null, new Patient(null, firstName, null));
        List<Encounter> peL5 = instance.search(pe3);
        Assert.assertTrue(peL5.size() > 0);
        for (Encounter pe : peL5) {
            if (pe.getPatient() != null) {
                Assert.assertEquals(pe.getPatient().getfN(), firstName);
            }
            Assert.assertEquals(pe.getpFN(), firstName);
        }

    }

    /**
     * @throws BlackBoxException
     */
    @Test
    public void testPatientsWithFirstAndLastName() throws BlackBoxException {
        // get some data that exists
        List<Encounter> allPEs = instance.search(new Encounter());
        int rId = Math.abs(random.nextInt() % allPEs.size());
        String firstName = allPEs.get(rId).getpFN();
        String lastName = allPEs.get(rId).getpLN();

        Patient p = new Patient();
        // All Patients with first name
        List<String> queryKeys = Arrays.asList(".*:" + firstName + ":.*", ".*:.*:" + lastName);
        List<Patient> pList5 = instance.fetchByPartialKey(queryKeys, p);
        for (Patient pat : pList5) {
            boolean result = firstName.equals(pat.getfN()) || lastName.equals(pat.getlN());
            Assert.assertTrue(result);
        }

        if (pList5.size() > 0) {
            int maxCount = pList5.size() / 3 + 1;
            List<Patient> pList6 = instance.fetchByPartialKey(queryKeys, p, maxCount);
            logger.info("" + pList5.size() + ":" + pList6.size() + ":" + maxCount);
            Assert.assertTrue(pList6.size() <= maxCount * 2, pList5.size() + ":" + pList6.size() + ":" + maxCount);
        }

    }

    /**
     * @throws BlackBoxException
     */
    @Test
    public void testAllClaimChargesForPatient() throws BlackBoxException {
        // Get a random patient id
        List<Patient> allPats = instance.search(new Patient());
        logger.info("All Patients :" + allPats);
        // get a random patient
        Patient p = allPats.get(Math.abs(random.nextInt() % allPats.size()));

        Claim cl = new Claim(null, null);
        cl.setpId(p.getpId());

        logger.info("Searching for cl " + cl + " ::: " + p);

        List<Claim> allClaims = instance.search(cl);
        double totalAmount = 0;
        for (Claim thisClaim : allClaims) {
            logger.info("Claim :" + thisClaim + " :" + thisClaim.getpId());
            Assert.assertEquals(thisClaim.getpId(), p.getpId(), thisClaim.toString());
            if (thisClaim.getcCharges() != null) {
                for (ClaimCharges cCharge : thisClaim.getcCharges()) {
                    totalAmount += cCharge.getAmount();
                }
            }
        }
        if (allClaims.size() > 0) {
            Assert.assertTrue(totalAmount > 0, totalAmount + "");
        }
    }

    /**
     * @throws BlackBoxException
     */
    @Test
    public void testAllClaimsForPatient() throws BlackBoxException {
        // Get a random patient id
        List<Patient> allPats = instance.search(new Patient());
        // get a random patient
        Patient p = allPats.get(Math.abs(random.nextInt() % allPats.size()));

        final Encounter encounter = new Encounter(null, new Patient(null, p.getfN(), null));

        List<Encounter> encList = instance.search(encounter);
        for (Encounter enc : encList) {
            Assert.assertEquals(p.getfN(), enc.getpFN());
        }
    }

    /**
     * @throws BlackBoxException
     */
    @Test
    public void testGetEncounterByPatient() throws BlackBoxException {
        Encounter pe = new Encounter();
        List<Encounter> peL1 = instance.search(Collections.singletonList(pe));
        int randId = random.nextInt(100000000) % peL1.size();

        String patientId = peL1.get(randId).getpId();
        String encId = peL1.get(randId).geteId();

        long startTime = System.nanoTime();
        List<Encounter> resultOne = instance.fetchByPartialKey(patientId, new Encounter());
        long endTime = System.nanoTime();
        long execTime = endTime - startTime;
        logger.info("1 :" + resultOne.size() + ":" + execTime);
        if (resultOne.size() > 0) {
            Assert.assertTrue(execTime / resultOne.size() < 10000000, "" + (execTime / resultOne.size()));
        }

        startTime = System.nanoTime();
        List<Encounter> resultTwo = instance.fetch(patientId + ":" + encId, new Encounter());
        endTime = System.nanoTime();
        execTime = endTime - startTime;
        logger.info("2 :" + resultTwo.size() + ":" + execTime);
        if (resultTwo.size() > 0) {
            Assert.assertTrue(execTime / resultTwo.size() < 10000000, "" + (execTime / resultTwo.size()));
        }

        startTime = System.nanoTime();
        List<Encounter> resultThree = instance.fetchByPartialKey(".*" + patientId + ":" + encId, new Encounter());
        endTime = System.nanoTime();
        execTime = endTime - startTime;
        logger.info("3 :" + resultThree.size() + ":" + execTime);
        if (resultThree.size() > 0) {
            Assert.assertTrue(execTime / resultThree.size() < 10000000, "" + (execTime / resultThree.size()));
        }

        startTime = System.nanoTime();
        List<Encounter> resultFour = instance.fetchByPartialKey(".*" + patientId + ":.*", new Encounter());
        endTime = System.nanoTime();
        execTime = endTime - startTime;
        logger.info("4 :" + resultFour.size() + ":" + execTime);
        if (resultFour.size() > 0) {
            Assert.assertTrue(execTime / resultFour.size() < 10000000, "" + (execTime / resultFour.size()));
        }

        startTime = System.nanoTime();
        List<Encounter> resultFive = instance.fetchByPartialKey(patientId + ":.*", new Encounter());
        endTime = System.nanoTime();
        execTime = endTime - startTime;
        logger.info("5 :" + resultFive.size() + ":" + execTime);
        if (resultFive.size() > 0) {
            Assert.assertTrue(execTime / resultFive.size() < 10000000, "" + (execTime / resultFive.size()));
        }

    }

    /**
     * @throws BlackBoxException
     */
    @Test
    public void testRangeSearch() throws BlackBoxException {
        final double minVal = random.nextDouble() * 100;
        final double maxVal = random.nextDouble() * 23443 + minVal;

        ClaimCharges startClaimCharge = new ClaimCharges(null, minVal);
        ClaimCharges endClaimCharge = new ClaimCharges(null, maxVal);
        logger.info("Search Charges:" + minVal + " to " + maxVal);
        List<ClaimCharges> all = instance.search(new ClaimCharges(null, null));
        int count = 0;
        for (ClaimCharges cc : all) {
            if (cc.getAmount() != null && cc.getAmount() >= minVal && cc.getAmount() < maxVal) {
                count++;
            }
        }

        List<ClaimCharges> resultOne = instance.search(startClaimCharge, endClaimCharge);
        logger.info("Hbase Charges :" + count + " :" + resultOne.size());
        Assert.assertEquals(resultOne.size(), count);
        for (ClaimCharges claimCharges : resultOne) {
            Assert.assertTrue(claimCharges.getAmount() >= minVal);
            Assert.assertTrue(claimCharges.getAmount() < maxVal);
        }

        // For only patients with id starting with 1
        startClaimCharge = new ClaimCharges("^1.*", minVal);
        endClaimCharge = new ClaimCharges(null, maxVal);
        logger.info("Search Charges:" + minVal + " to " + maxVal);
        all = instance.search(new ClaimCharges(null, null));
        count = 0;
        for (ClaimCharges cc : all) {
            if (cc.getAmount() != null && cc.getCcId().startsWith("1") && cc.getAmount() >= minVal && cc.getAmount() < maxVal) {
                count++;
            }
        }

        resultOne = instance.search(startClaimCharge, endClaimCharge);

        logger.info("Charges :" + count + " :" + resultOne.size());
        Assert.assertEquals(resultOne.size(), count);
        for (ClaimCharges claimCharges : resultOne) {
            Assert.assertTrue(claimCharges.getAmount() >= minVal);
            Assert.assertTrue(claimCharges.getAmount() < maxVal);
        }

    }
}
