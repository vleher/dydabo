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
package com.dydabo.test.blackbox.usecase.medical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.test.blackbox.usecase.medical.db.Claim;
import com.dydabo.test.blackbox.usecase.medical.db.ClaimCharges;
import com.dydabo.test.blackbox.usecase.medical.db.Encounter;
import com.dydabo.test.blackbox.usecase.medical.db.Medication;
import com.dydabo.test.blackbox.usecase.medical.db.Patient;
import com.dydabo.test.blackbox.utils.DyDaBoTestUtils;

/**
 * @author viswadas leher
 */
public abstract class MedicalUseCaseTest {
	/**
	 *
	 */
	public static final List<String> Diagnosis = Arrays.asList("Diabetes", "High Blood Pressure", "Low Blood Pressure",
			"Polio", "Fever", "Common Cold", "Allergy");
	/**
	 *
	 */
	public static final List<String> Meds = Arrays.asList("Acetylmethadol", "Benzethidine", "Difenoxin", "Furethidine",
			"Phenoperidine");
	/**
	 *
	 */
	public static List<String> FirstNames = Arrays.asList("David", "Peter", "Tom", "Dick", "Harry", "John", "Bill",
			"Adele", "Britney", "Mariah", "Tina", "Diana", "Dionne", "Cyndi", "Kim", "Lindsey", "Shiela", "Bette");
	/**
	 *
	 */
	public static List<String> LastNames = Arrays.asList("Johnson", "Becker", "Smith", "Gates", "King", "Spears",
			"Perry", "Carey", "Gomez", "Lopez", "Turner", "Ross", "Warwick", "Lauper", "Carnes", "Midler", "Jackson",
			"Hayes");
	protected final DyDaBoTestUtils utils = new DyDaBoTestUtils();
	protected final int knownPatientId = 123456;
	protected final Random random = new Random();
	private final Logger logger = LogManager.getLogger();
	protected BlackBox<Patient> patientBlackBox;
	protected BlackBox<Encounter> encounterBlackBox;
	protected BlackBox<Medication> medicationBlackBox;
	protected BlackBox<ClaimCharges> claimChargesBlackBox;
	protected BlackBox<Claim> claimBlackBox;

	/**
	 * @throws BlackBoxException
	 * @throws IOException
	 */
	public MedicalUseCaseTest() throws BlackBoxException, IOException {
		patientBlackBox = null;
		encounterBlackBox = null;
		medicationBlackBox = null;
		claimChargesBlackBox = null;
	}

	/**
	 * @throws BlackBoxException
	 */
	public void testPatientWithId() throws BlackBoxException {
		// All Patients with patient id (row key search)
		final String pIDKey = "123456P:.*";
		final List<Patient> pList2 = patientBlackBox.fetchByPartialKey(new Patient(pIDKey, ".*", ".*"));
		for (final Patient pat : pList2) {
			assertEquals(pat.getPatientId(), "123456P");
		}
	}

	public void testAllPatientEncounters() throws BlackBoxException {
		// All Encounters
		final Encounter pe = new Encounter();
		final List<Encounter> peL1 = encounterBlackBox.search(Collections.singletonList(pe));
		assertTrue(peL1.size() > 0, "" + peL1.size());

		final List<Encounter> peL2 = encounterBlackBox.search(pe, 7, false);
		assertTrue((peL2.size() <= 7), "" + peL2.size());

		final List<Encounter> peL3 = encounterBlackBox.search(pe, 23, false);
		assertTrue(peL3.size() <= 23, "" + peL3.size());
	}

	public void testAllPatients() throws BlackBoxException {
		// All Patients
		final Patient p = new Patient();
		final List<Patient> pList1 = patientBlackBox.search(Collections.singletonList(p));
		assertTrue(pList1.size() > 0);
	}

	public void testFetchQueryPerformance() throws BlackBoxException {
		// get some data that exists
		final List<Encounter> allPEs = encounterBlackBox.search(new Encounter());
		final int rId = Math.abs(random.nextInt() % allPEs.size());
		logger.info("Performace Test rId : {} : {}", rId, allPEs.size());
		final String firstName = allPEs.get(rId).getPatientFirstName();
		final String lastName = allPEs.get(rId).getPatientLastName();

		long startTime = System.nanoTime();
		final List<Patient> p4 = patientBlackBox.fetchByPartialKey(new Patient(".*", firstName, lastName));
		long endTime = System.nanoTime();
		logger.info("Performance Test 1 : {} : {}" + p4.size(), (endTime - startTime));

		startTime = System.nanoTime();
		final List<Patient> p5 = patientBlackBox.fetchByPartialKey(new Patient(".*", ".*", lastName));
		endTime = System.nanoTime();
		logger.info("Performance Test 2 : {} : {}" + p5.size(), (endTime - startTime));

		startTime = System.nanoTime();
		final List<Patient> p6 = patientBlackBox.fetchByPartialKey(new Patient(".*", firstName, ".*"));
		endTime = System.nanoTime();
		logger.info("Performance Test 3 : {} : {}" + p6.size(), (endTime - startTime));
	}

	public void testPatientByName() throws BlackBoxException {
		// All Patients with a specific first name (column value search)
		final Patient p = new Patient();
		p.setFirstName("Tina");
		final List<Patient> pList3 = patientBlackBox.search(Collections.singletonList(p));
		for (final Patient pat : pList3) {
			assertEquals(pat.getFirstName(), "Tina");
		}

		// All Patients with specific first name (row key search)
		final Patient patient = new Patient();
		patient.setPatientId(".*");
		patient.setFirstName("Cyndi");
		patient.setLastName(".*");
		final List<Patient> pList4 = patientBlackBox.fetchByPartialKey(patient);
		for (final Patient pat : pList4) {
			assertEquals(pat.getFirstName(), "Cyndi");
		}
	}

	public void testPatientEncountersById() throws BlackBoxException {
		// All encounters by patient Id (row key)
		final Encounter encounter = new Encounter();
		encounter.setPatientId(String.valueOf(knownPatientId));
		encounter.setEncounterId(".*");
		final List<Encounter> peL2 = encounterBlackBox.fetchByPartialKey(encounter);
		for (final Encounter penc : peL2) {
			assertEquals(penc.getPatientId(), knownPatientId + "P");
		}
	}

	public void testPatientEncountersByMedId() throws BlackBoxException {
		// All encounters with a medication id
		final List<Medication> med1 = medicationBlackBox.search(new Medication());
		if (med1.size() > 0) {
			final String randMedId = med1.get(Math.abs(random.nextInt() % med1.size())).getmId();
			final Encounter pe2 = new Encounter();
			pe2.setMedicationIds(".*" + randMedId + ".*");

			final List<Encounter> encs = encounterBlackBox.search(pe2);
			for (final Encounter enc : encs) {
				assertTrue(enc.getMedicationIds().contains(randMedId), enc.getMedicationIds());
				if (enc.getPatient() != null) {
					assertTrue(!enc.getPatient().getFirstName().isEmpty(), enc.getPatient().toString());
				}
			}
		}
	}

	public void testPatientEncountersByMedIdAndPFName() throws BlackBoxException {
		// get a patient
		final List<Patient> allPatients = patientBlackBox.search(new Patient());
		assertTrue(allPatients.size() > 0);

		// random patient first name
		final String firstName = allPatients.get(Math.abs(random.nextInt() % allPatients.size())).getFirstName();
		// get a medication name
		final List<Medication> allMeds = medicationBlackBox.search(new Medication());
		// random medication name
		final String medId = allMeds.get(Math.abs(random.nextInt() % allMeds.size())).getmId();
		// search encounters
		final Encounter pe = new Encounter();
		pe.setPatientFirstName(firstName);
		pe.setMedicationIds(".*" + medId + ".*");
		final List<Encounter> results = encounterBlackBox.search(pe);

		for (final Encounter r : results) {
			if (r.getPatient() != null) {
				assertEquals(r.getPatient().getFirstName(), firstName, r.getPatient().toString());
			}
			assertEquals(r.getPatientFirstName(), firstName, r.toString());
			assertTrue(r.getMedicationIds().contains(medId), r.getMedicationIds());
		}
	}

	public void testAllPatientsWithDiagnosis() throws BlackBoxException {

		final String diagId = Diagnosis.get(Math.abs(random.nextInt() % Diagnosis.size()));

		final Encounter pe = new Encounter();
		pe.setDiagnosisIds(".*" + diagId + ".*");

		final List<Encounter> peList = encounterBlackBox.search(pe);
		for (final Encounter enc : peList) {
			assertTrue(enc.getDiagnosisIds().contains(diagId));
		}
	}

	public void testPatientEncountersByName() throws BlackBoxException {

		// get some data that exists
		final List<Encounter> allPEs = encounterBlackBox.search(new Encounter());
		final int rId = Math.abs(random.nextInt() % allPEs.size());
		final String firstName = allPEs.get(rId).getPatientFirstName();
		final String lastName = allPEs.get(rId).getPatientLastName();

		logger.info("First Name : {} :Last Name: {} => All Encounters: {}", firstName, lastName, allPEs.get(rId));

		// All encounters for patient names
		final Patient p1 = new Patient();
		p1.setFirstName(firstName);
		p1.setLastName(lastName);
		final List<Patient> p2 = patientBlackBox.search(p1);
		assertTrue(p2.size() > 0, "Size :" + p2.size());
		final List<Encounter> peL3 = new ArrayList<>();
		for (final Patient patient : p2) {
			final List<Encounter> temp = encounterBlackBox
					.fetchByPartialKey(new Encounter("*", patient.getPatientId()));
			peL3.addAll(temp);
		}

		assertTrue(peL3.size() > 0, "Size:" + peL3.size());
		for (final Encounter pe : peL3) {
			assertEquals(pe.getPatientFirstName(), firstName);
			assertEquals(pe.getPatientLastName(), lastName);
			if (pe.getPatient() != null) {
				assertEquals(pe.getPatient().getFirstName(), firstName, pe.getPatient().toString());
				assertEquals(pe.getPatient().getLastName(), lastName, pe.getPatient().toString());
			}
		}

		final Encounter pe2 = new Encounter(null, null, firstName, lastName);
		final List<Encounter> peL4 = encounterBlackBox.search(pe2);
		assertTrue(peL4.size() > 0);
		assertEquals(peL4.size(), peL3.size());

		for (final Encounter pe : peL4) {
			assertEquals(pe.getPatientFirstName(), firstName);
			assertEquals(pe.getPatientLastName(), lastName);
			if (pe.getPatient() != null) {
				assertEquals(pe.getPatient().getFirstName(), firstName);
				assertEquals(pe.getPatient().getLastName(), lastName);
			}
		}

		final Encounter pe3 = new Encounter(null, new Patient(null, firstName, null));
		final List<Encounter> peL5 = encounterBlackBox.search(pe3);
		assertTrue(peL5.size() > 0);
		for (final Encounter pe : peL5) {
			if (pe.getPatient() != null) {
				assertEquals(pe.getPatient().getFirstName(), firstName);
			}
			assertEquals(pe.getPatientFirstName(), firstName);
		}
	}

	/**
	 * @throws BlackBoxException
	 */
	public void testPatientsWithFirstAndLastName() throws BlackBoxException {
		// get some data that exists
		final List<Encounter> allPEs = encounterBlackBox.search(new Encounter());
		final int rId = Math.abs(random.nextInt() % allPEs.size());
		final String firstName = allPEs.get(rId).getPatientFirstName();
		final String lastName = allPEs.get(rId).getPatientLastName();

		// All Patients with first name
		final Patient p1 = new Patient();
		p1.setFirstName(firstName);
		final Patient p2 = new Patient();
		p2.setLastName(lastName);
		final List<Patient> pList5 = patientBlackBox.fetchByPartialKey(Arrays.asList(p1, p2));
		for (final Patient pat : pList5) {
			final boolean result = firstName.equals(pat.getFirstName()) || lastName.equals(pat.getLastName());
			assertTrue(result);
		}

		if (pList5.size() > 0) {
			final int maxCount = (pList5.size() / 3) + 1;
			final List<Patient> pList6 = patientBlackBox.fetchByPartialKey(Arrays.asList(p1, p2), maxCount, false);
			assertTrue(pList6.size() <= (maxCount * 2), pList5.size() + ":" + pList6.size() + ":" + maxCount);
		}
	}

	/**
	 * @throws BlackBoxException
	 */
	public void testAllClaimChargesForPatient() throws BlackBoxException {
		// Get a random patient id
		final List<Patient> allPats = patientBlackBox.search(new Patient());
		// get a random patient
		final Patient p = allPats.get(Math.abs(random.nextInt() % allPats.size()));

		final Claim cl = new Claim(null, null);
		cl.setpId(p.getPatientId());

		final List<Claim> allClaims = claimBlackBox.search(cl);
		double totalAmount = 0;
		for (final Claim thisClaim : allClaims) {
			assertEquals(thisClaim.getpId(), p.getPatientId(), thisClaim.toString());
			if (thisClaim.getcCharges() != null) {
				for (final ClaimCharges cCharge : thisClaim.getcCharges()) {
					totalAmount += cCharge.getAmount();
				}
			}
		}
		if (allClaims.size() > 0) {
			assertTrue(totalAmount > 0, totalAmount + "");
		}
	}

	/**
	 * @throws BlackBoxException
	 */
	public void testAllClaimsForPatient() throws BlackBoxException {
		// Get a random patient id
		final List<Patient> allPats = patientBlackBox.search(new Patient());
		// get a random patient
		final Patient p = allPats.get(Math.abs(random.nextInt() % allPats.size()));

		final Encounter encounter = new Encounter(null, new Patient(null, p.getFirstName(), null));

		final List<Encounter> encList = encounterBlackBox.search(encounter);
		for (final Encounter enc : encList) {
			assertEquals(p.getFirstName(), enc.getPatientFirstName());
		}
	}

	/**
	 * @throws BlackBoxException
	 */
	public void testGetEncounterByPatient() throws BlackBoxException {
		final long expectedTimeNS = (long) (5L * 1e+8); // 500 millisecs
		final Encounter pe = new Encounter();
		final List<Encounter> peL1 = encounterBlackBox.search(Collections.singletonList(pe));
		final int randId = random.nextInt(2984745) % peL1.size();

		final String patientId = peL1.get(randId).getPatientId();
		final String encId = peL1.get(randId).getEncounterId();
		logger.info("Fetching by {} {} {}", patientId, encId, peL1);

		long startTime = System.nanoTime();
		final List<Encounter> resultOne = encounterBlackBox.fetchByPartialKey(new Encounter("", patientId));
		long endTime = System.nanoTime();
		long execTime = endTime - startTime;
		logger.info("Performance 1 size:{} Time:{} ns", resultOne.size(), execTime);
		assertTrue(execTime < expectedTimeNS, (execTime / 1e6) + "ms");

		startTime = System.nanoTime();
		final List<Encounter> resultTwo = encounterBlackBox.fetch(new Encounter(encId, patientId));
		endTime = System.nanoTime();
		execTime = endTime - startTime;
		logger.info("Performance 2 size:{} Time:{} ns", resultTwo.size(), execTime);
		assertTrue(execTime < expectedTimeNS, (execTime / 1e6) + "ms");

		startTime = System.nanoTime();
		final Encounter s = new Encounter(encId, ".*" + patientId);
		final List<Encounter> resultThree = encounterBlackBox.fetchByPartialKey(s);
		endTime = System.nanoTime();
		execTime = endTime - startTime;
		logger.info("Performance 3 size:{} Time:{} ns", resultThree.size(), execTime);
		assertTrue(execTime < expectedTimeNS, (execTime / 1e6) + "ms");

		startTime = System.nanoTime();
		final List<Encounter> resultFour = encounterBlackBox.fetchByPartialKey(new Encounter(".*", ".*" + patientId));
		endTime = System.nanoTime();
		execTime = endTime - startTime;
		logger.info("Performance 4 size:{} Time:{} ns", resultFour.size(), execTime);
		assertTrue(execTime < expectedTimeNS, (execTime / 1e6) + "ms");

		startTime = System.nanoTime();
		final List<Encounter> resultFive = encounterBlackBox.fetchByPartialKey(new Encounter(".*", patientId));
		endTime = System.nanoTime();
		execTime = endTime - startTime;
		logger.info("Performance 5 size:{} Time:{} ns", resultFive.size(), execTime);
		assertTrue(execTime < expectedTimeNS, (execTime / 1e6) + "ms");
	}

	/**
	 * @throws BlackBoxException
	 */
	public void testRangeSearch() throws BlackBoxException {
		final double minVal = random.nextDouble() * 100;
		final double maxVal = (random.nextDouble() * 23443) + minVal;

		ClaimCharges startClaimCharge = new ClaimCharges(null, minVal);
		ClaimCharges endClaimCharge = new ClaimCharges(null, maxVal);
		final List<ClaimCharges> all = claimChargesBlackBox.search(new ClaimCharges(null, null));
		int count = 0;
		for (final ClaimCharges cc : all) {
			if ((cc.getAmount() != null) && (cc.getAmount() >= minVal) && (cc.getAmount() < maxVal)) {
				count++;
			}
		}

		List<ClaimCharges> resultOne = claimChargesBlackBox.search(startClaimCharge, endClaimCharge);
		assertEquals(resultOne.size(), count);
		for (final ClaimCharges claimCharges : resultOne) {
			assertTrue(claimCharges.getAmount() >= minVal);
			assertTrue(claimCharges.getAmount() < maxVal);
		}

		// For only patients with id starting with 1
		startClaimCharge = new ClaimCharges("^1.*", minVal);
		endClaimCharge = new ClaimCharges("^1.*", maxVal);

		count = 0;
		for (final ClaimCharges cc : all) {
			if ((cc.getAmount() != null) && cc.getCcId().startsWith("1") && (cc.getAmount() >= minVal)
					&& (cc.getAmount() < maxVal)) {
				count++;
			}
		}

		resultOne = claimChargesBlackBox.search(startClaimCharge, endClaimCharge);

		assertEquals(resultOne.size(), count);
		for (final ClaimCharges claimCharges : resultOne) {
			assertTrue(claimCharges.getAmount() >= minVal);
			assertTrue(claimCharges.getAmount() < maxVal);
		}
	}

	protected void testMaxResultsSearch() throws BlackBoxException {
		final Patient patient = new Patient();
		final Random random = new Random();
		List<Patient> patients = new ArrayList<>();
		String firstName = "Initial";
		while (patients.size() <= 0) {
			firstName = FirstNames.get(random.nextInt(FirstNames.size()));
			patient.setFirstName(firstName);
			patients = patientBlackBox.search(patient);
		}

		final int resultCount = patients.size() / 2;
		logger.info("Searching Patients: {} : total size:{} result count:{}", firstName, patients.size(), resultCount);
		final List<Patient> firstPatients = patientBlackBox.search(patient, resultCount, true);
		assertTrue(firstPatients.size() == resultCount);
		int i = 0;
		for (final Patient p : firstPatients) {
			assertEquals(patients.get(i++), p);
		}

		final List<Patient> lastPatients = patientBlackBox.search(patient, resultCount, false);
		i = patients.size() - resultCount;
		assertTrue(lastPatients.size() == resultCount);
		for (final Patient p : lastPatients) {
			assertEquals(patients.get(i++), p);
		}
	}
}
