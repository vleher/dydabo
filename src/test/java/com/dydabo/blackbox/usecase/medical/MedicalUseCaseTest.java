/** *****************************************************************************
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 */
package com.dydabo.blackbox.usecase.medical;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.hbase.utils.DyDaBoTestUtils;
import com.dydabo.blackbox.usecase.medical.db.Claim;
import com.dydabo.blackbox.usecase.medical.db.ClaimCharges;
import com.dydabo.blackbox.usecase.medical.db.Encounter;
import com.dydabo.blackbox.usecase.medical.db.Medication;
import com.dydabo.blackbox.usecase.medical.db.Patient;
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

    private BlackBox blackBox;

    /**
     *
     */
    public static List<String> Diagnosis = Arrays.asList("Diabetes", "High Blood Pressure", "Low Blood Pressure", "Polio", "Fever",
            "Common Cold", "Allergy");

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

    /**
     *
     */
    public static List<String> Meds = Arrays.asList("Acetylmethadol", "Benzethidine", "Difenoxin", "Furethidine", "Phenoperidine");

    int knownPatientId = 123456;
    Random random = new Random();
    DyDaBoTestUtils testUtils = new DyDaBoTestUtils();

    /**
     *
     * @throws BlackBoxException
     * @throws IOException
     */
    public MedicalUseCaseTest() throws BlackBoxException, IOException {
        blackBox = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
        // Pre-populate with some dynamic data.
        testUtils.generatePatients(random.nextInt(98765) % 10);
        testUtils.generateEncounters(random.nextInt(98765) % 10);
    }

    /**
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUpClass() throws Exception {

    }

    /**
     *
     * @throws Exception
     */
    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @BeforeMethod
    public void setUpMethod() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testAllPatientEncounters() throws BlackBoxException {
        // All Encounters
        Encounter pe = new Encounter();
        List<Encounter> peL1 = blackBox.search(Arrays.asList(pe));
        Assert.assertTrue("" + peL1.size(), peL1.size() > 0);

        List<Encounter> peL2 = blackBox.search(pe, 7);
        Assert.assertTrue("" + peL2.size(), peL2.size() <= 7);

        List<Encounter> peL3 = blackBox.search(pe, 23);
        Assert.assertTrue("" + peL3.size(), peL3.size() <= 23);
    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testAllPatients() throws BlackBoxException {
        // All Patients
        Patient p = new Patient();
        List<Patient> pList1 = blackBox.search(Arrays.asList(p));
        Assert.assertTrue(pList1.size() > 0);
    }

    @Test
    public void testFetchQueryPerformance() throws BlackBoxException {
        // get some data that exists
        List<Encounter> allPEs = blackBox.search(new Encounter());
        int rId = Math.abs(random.nextInt() % allPEs.size());
        System.out.println("rId :" + rId + ":" + allPEs.size());
        String firstName = allPEs.get(rId).getpFN();
        String lastName = allPEs.get(rId).getpLN();

        long startTime = System.nanoTime();
        List<Patient> p4 = blackBox.fetchByPartialKey(".*:" + firstName + ":" + lastName, new Patient());
        long endTime = System.nanoTime();
        System.out.println("1 :" + p4.size() + ":" + (endTime - startTime));

        startTime = System.nanoTime();
        List<Patient> p5 = blackBox.fetchByPartialKey(".*:" + ".*:" + lastName, new Patient());
        endTime = System.nanoTime();
        System.out.println("2 :" + p5.size() + ":" + (endTime - startTime));

        startTime = System.nanoTime();
        List<Patient> p6 = blackBox.fetchByPartialKey(".*:" + firstName + ":.*", new Patient());
        endTime = System.nanoTime();
        System.out.println("3 :" + p6.size() + ":" + (endTime - startTime));
    }

    /**
     *
     * @throws BlackBoxException
     */
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

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testPatientEncountersById() throws BlackBoxException {
        Encounter pe = new Encounter();
        // All encounters by patient Id (row key)
        List<String> query = Arrays.asList(knownPatientId + "P:.*");
        List<Encounter> peL2 = blackBox.fetchByPartialKey(query, pe);
        for (Encounter penc : peL2) {
            Assert.assertEquals(penc.getpId(), knownPatientId + "P");
        }

    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testPatientEncountersByMedId() throws BlackBoxException {

        // All encounters with a medication id
        List<Medication> med1 = blackBox.search(new Medication());
        if (med1.size() > 0) {
            String randMedId = med1.get(Math.abs(random.nextInt() % med1.size())).getmId();
            Encounter pe2 = new Encounter();
            pe2.setMedIds(".*" + randMedId + ".*");

            List<Encounter> encs = blackBox.search(pe2);
            for (Encounter enc : encs) {
                Assert.assertTrue(enc.getMedIds(), enc.getMedIds().contains(randMedId));
                if (enc.getPatient() != null) {
                    Assert.assertTrue(enc.getPatient().toString(), !enc.getPatient().getfN().isEmpty());
                }
            }
        }
    }

    /**
     *
     * @throws BlackBoxException
     */
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
        Encounter pe = new Encounter();
        pe.setpFN(firstName);
        pe.setMedIds(".*" + medId + ".*");
        List<Encounter> results = blackBox.search(pe);

        for (Encounter r : results) {
            if (r.getPatient() != null) {
                Assert.assertEquals(r.getPatient().toString(), r.getPatient().getfN(), firstName);
            }
            Assert.assertEquals(r.toString(), r.getpFN(), firstName);
            Assert.assertTrue(r.getMedIds(), r.getMedIds().contains(medId));
        }
    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testAllPatientsWithDiagnosis() throws BlackBoxException {

        String diagId = Diagnosis.get(Math.abs(random.nextInt() % Diagnosis.size()));

        Encounter pe = new Encounter();
        pe.setDiagIds(".*" + diagId + ".*");

        List<Encounter> peList = blackBox.search(pe);
        for (Encounter enc : peList) {
            Assert.assertTrue(enc.getDiagIds().contains(diagId));
        }

    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testPatientEncountersByName() throws BlackBoxException {

        // get some data that exists
        List<Encounter> allPEs = blackBox.search(new Encounter());
        int rId = Math.abs(random.nextInt() % allPEs.size());
        String firstName = allPEs.get(rId).getpFN();
        String lastName = allPEs.get(rId).getpLN();

        Encounter pe1 = new Encounter();
        // All encounters for patient names
        List<Patient> p2 = blackBox.fetchByPartialKey(".*:" + firstName + ":" + lastName, new Patient());
        List<Encounter> peL3 = new ArrayList<>();
        for (Patient patient : p2) {
            List<Encounter> temp = blackBox.fetchByPartialKey(patient.getpId() + ":.*", pe1);
            peL3.addAll(temp);
        }

        Assert.assertTrue(peL3.size() > 0);
        for (Encounter pe : peL3) {
            Assert.assertEquals(pe.getpFN(), firstName);
            Assert.assertEquals(pe.getpLN(), lastName);
            if (pe.getPatient() != null) {
                Assert.assertEquals(pe.getPatient().toString(), pe.getPatient().getfN(), firstName);
                Assert.assertEquals(pe.getPatient().toString(), pe.getPatient().getlN(), lastName);
            }
        }

        Encounter pe2 = new Encounter(null, null, firstName, lastName);
        List<Encounter> peL4 = blackBox.search(pe2);
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
        List<Encounter> peL5 = blackBox.search(pe3);
        Assert.assertTrue(peL5.size() > 0);
        for (Encounter pe : peL5) {
            if (pe.getPatient() != null) {
                Assert.assertEquals(pe.getPatient().getfN(), firstName);
            }
            Assert.assertEquals(pe.getpFN(), firstName);
        }

    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testPatientWithId() throws BlackBoxException {
        Patient p = new Patient();
        // All Patients with patient id (row key search)
        String pIDKey = "123456P:.*";
        List<Patient> pList2 = blackBox.fetchByPartialKey(pIDKey, p);
        for (Patient pat : pList2) {
            Assert.assertEquals(pat.getpId(), "123456P");
        }
    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testPatientsWithFirstAndLastName() throws BlackBoxException {
        // get some data that exists
        List<Encounter> allPEs = blackBox.search(new Encounter());
        int rId = Math.abs(random.nextInt() % allPEs.size());
        String firstName = allPEs.get(rId).getpFN();
        String lastName = allPEs.get(rId).getpLN();

        Patient p = new Patient();
        // All Patients with first name
        List<String> queryKeys = Arrays.asList(".*:" + firstName + ":.*", ".*:.*:" + lastName);
        List<Patient> pList5 = blackBox.fetchByPartialKey(queryKeys, p);
        for (Patient pat : pList5) {
            boolean result = firstName.equals(pat.getfN()) || lastName.equals(pat.getlN());
            Assert.assertTrue(result);
        }

        if (pList5.size() > 0) {
            int maxCount = pList5.size() / 3;
            List<Patient> pList6 = blackBox.fetchByPartialKey(queryKeys, p, maxCount);
            System.out.println("" + pList5.size() + ":" + pList6.size() + ":" + maxCount);
            Assert.assertTrue(pList5.size() + ":" + pList6.size() + ":" + maxCount, pList6.size() <= maxCount * 2);
        }

    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testAllClaimChargesForPatient() throws BlackBoxException {
        // Get a random patient id
        List<Patient> allPats = blackBox.search(new Patient());
        // get a random patient
        Patient p = allPats.get(Math.abs(random.nextInt() % allPats.size()));

        Claim cl = new Claim(null, null);
        cl.setpId(p.getpId());

        List<Claim> allClaims = blackBox.search(cl);
        double totalAmount = 0;
        for (Claim thisClaim : allClaims) {
            Assert.assertEquals(thisClaim.toString(), p.getpId(), thisClaim.getpId());
            System.out.println("Claim :" + thisClaim);
            for (ClaimCharges cCharge : thisClaim.getcCharges()) {
                totalAmount += cCharge.getAmount();
            }
        }
        if (allClaims.size() > 0) {
            Assert.assertTrue(totalAmount + "", totalAmount > 0);
        }
    }

    @Test
    public void testAllClaimsForPatient() throws BlackBoxException {
        // Get a random patient id
        List<Patient> allPats = blackBox.search(new Patient());
        // get a random patient
        Patient p = allPats.get(Math.abs(random.nextInt() % allPats.size()));

        final Encounter encounter = new Encounter(null, new Patient(null, p.getfN(), null));

        List<Encounter> encList = blackBox.search(encounter);
        for (Encounter enc : encList) {
            Assert.assertEquals(p.getfN(), enc.getpFN());
        }
    }

    @Test
    public void testGetEncounterByPatient() throws BlackBoxException {
        Encounter pe = new Encounter();
        List<Encounter> peL1 = blackBox.search(Arrays.asList(pe));
        int randId = random.nextInt(100000000) % peL1.size();

        String patientId = peL1.get(randId).getpId();
        String encId = peL1.get(randId).geteId();

        long startTime = System.nanoTime();
        List<Encounter> resultOne = blackBox.fetchByPartialKey(patientId, new Encounter());
        long endTime = System.nanoTime();
        long execTime = endTime - startTime;
        System.out.println("1 :" + resultOne.size() + ":" + execTime);
        if (resultOne.size() > 0) {
            Assert.assertTrue(execTime / resultOne.size() < 50000000);
        }

        startTime = System.nanoTime();
        List<Encounter> resultTwo = blackBox.fetch(patientId + ":" + encId, new Encounter());
        endTime = System.nanoTime();
        execTime = endTime - startTime;
        System.out.println("2 :" + resultTwo.size() + ":" + execTime);
        if (resultTwo.size() > 0) {
            Assert.assertTrue(execTime / resultTwo.size() < 50000000);
        }

        startTime = System.nanoTime();
        List<Encounter> resultThree = blackBox.fetchByPartialKey(".*" + patientId + ":" + encId, new Encounter());
        endTime = System.nanoTime();
        execTime = endTime - startTime;
        System.out.println("3 :" + resultThree.size() + ":" + execTime);
        if (resultThree.size() > 0) {
            Assert.assertTrue(execTime / resultThree.size() < 50000000);
        }

        startTime = System.nanoTime();
        List<Encounter> resultFour = blackBox.fetchByPartialKey(".*" + patientId + ":.*", new Encounter());
        endTime = System.nanoTime();
        execTime = endTime - startTime;
        System.out.println("4 :" + resultFour.size() + ":" + execTime);
        if (resultFour.size() > 0) {
            Assert.assertTrue(execTime / resultFour.size() < 50000000);
        }

        startTime = System.nanoTime();
        List<Encounter> resultFive = blackBox.fetchByPartialKey(patientId + ":.*", new Encounter());
        endTime = System.nanoTime();
        execTime = endTime - startTime;
        System.out.println("5 :" + resultFive.size() + ":" + execTime);
        if (resultFive.size() > 0) {
            Assert.assertTrue(execTime / resultFive.size() < 50000000);
        }

    }

    @Test
    public void testRangeSearch() throws BlackBoxException {
        final double minVal = random.nextDouble() * 100;
        final double maxVal = random.nextDouble() * 23443 + minVal;

        ClaimCharges startClaimCharge = new ClaimCharges(null, minVal);
        ClaimCharges endClaimCharge = new ClaimCharges(null, maxVal);
        System.out.println("Search Charges:" + minVal + " to " + maxVal);
        List<ClaimCharges> all = blackBox.search(new ClaimCharges(null, null));
        int count = 0;
        for (ClaimCharges cc : all) {
            if (cc.getAmount() >= minVal && cc.getAmount() < maxVal) {
                count++;
            }
        }

        List<ClaimCharges> resultOne = blackBox.search(startClaimCharge, endClaimCharge);
        System.out.println("Charges :" + count + " :" + resultOne.size());
        Assert.assertEquals(count, resultOne.size());
        for (ClaimCharges claimCharges : resultOne) {
            Assert.assertTrue(claimCharges.getAmount() >= minVal);
            Assert.assertTrue(claimCharges.getAmount() < maxVal);
        }
        // For only patients with id starting with 1
        startClaimCharge = new ClaimCharges("^1.*", minVal);
        endClaimCharge = new ClaimCharges("^2.*", maxVal);
        System.out.println("Search Charges:" + minVal + " to " + maxVal);
        all = blackBox.search(new ClaimCharges(null, null));
        count = 0;
        for (ClaimCharges cc : all) {
            if (cc.getCcId().startsWith("1") && cc.getAmount() >= minVal && cc.getAmount() < maxVal) {
                count++;
            }
        }

        resultOne = blackBox.search(startClaimCharge, endClaimCharge);

        System.out.println("Charges :" + count + " :" + resultOne.size());
        Assert.assertEquals(count, resultOne.size());
        for (ClaimCharges claimCharges : resultOne) {
            Assert.assertTrue(claimCharges.getAmount() >= minVal);
            Assert.assertTrue(claimCharges.getAmount() < maxVal);
        }

    }
}
