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
package com.dydabo.test.blackbox.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.test.blackbox.BlackBoxFactory;
import com.dydabo.test.blackbox.usecase.company.Customer;
import com.dydabo.test.blackbox.usecase.company.Employee;
import com.dydabo.test.blackbox.usecase.medical.MedicalUseCaseTest;
import com.dydabo.test.blackbox.usecase.medical.db.Claim;
import com.dydabo.test.blackbox.usecase.medical.db.ClaimCharges;
import com.dydabo.test.blackbox.usecase.medical.db.ClaimDetails;
import com.dydabo.test.blackbox.usecase.medical.db.Diagnosis;
import com.dydabo.test.blackbox.usecase.medical.db.Encounter;
import com.dydabo.test.blackbox.usecase.medical.db.Medication;
import com.dydabo.test.blackbox.usecase.medical.db.Patient;

/**
 * @author viswadas leher
 */
public class DyDaBoTestUtils {

	public final List<BlackBoxFactory.Databases> dbToTest = Arrays.asList(BlackBoxFactory.Databases.REDIS,
			BlackBoxFactory.Databases.HBASE, BlackBoxFactory.Databases.MONGODB);

	/**
	 * Random first names
	 */
	public final List<String> FirstNames = Arrays.asList("David", "Peter", "Tom", "Dick", "Harry", "John", "Bill",
			"Adele", "Britney", "Mariah");

	/**
	 * Random last names
	 */
	public final List<String> LastNames = Arrays.asList("Johnson", "Becker", "Smith", "Gates", "King", "Spears",
			"Perry", "Carey", "Gomez", "Lopez");

	/**
	 * @param maxNumber
	 * @return
	 */
	public List<Customer> generateCustomers(final int maxNumber) {
		final List<Customer> custList = new ArrayList<>();
		final Random random = new Random();
		for (int i = 0; i < maxNumber; i++) {
			final int id = Math.abs(random.nextInt());
			final Customer customer = new Customer();
			customer.setUserId(id);
			final String firstName = FirstNames.get(Math.abs(id % FirstNames.size()));
			customer.setFirstName(firstName);
			final String lastName = LastNames.get(Math.abs(id % LastNames.size()));
			customer.setLastName(lastName);
			customer.setUserName(firstName.toLowerCase() + "." + lastName.toLowerCase());
			customer.setTaxRate(random.nextDouble() * 100);
			customer.initData();
			custList.add(customer);
		}
		return custList;
	}

	/**
	 * @param maxNumber
	 * @return
	 */
	public List<Employee> generateEmployees(final int maxNumber) {
		final List<Employee> userList = new ArrayList<>();
		final Random random = new Random();
		for (int i = 0; i < maxNumber; i++) {
			final int id = Math.abs(random.nextInt());
			final Employee employee = new Employee();
			employee.setUserId(id);
			final String firstName = FirstNames.get(Math.abs(id % FirstNames.size()));
			employee.setFirstName(firstName);
			final String lastName = LastNames.get(Math.abs(id % LastNames.size()));
			employee.setLastName(lastName);
			employee.setUserName(firstName.toLowerCase() + "." + lastName.toLowerCase());
			userList.add(employee);
		}
		return userList;
	}

	/**
	 * @param count
	 * @param diagnosisBlackBox
	 * @param medicationBlackBox
	 * @param claimBlackBox
	 * @param claimChargesBlackBox
	 * @param claimDetailsBlackBox
	 * @param encounterBlackBox
	 * @throws BlackBoxException
	 * @throws IOException
	 */
	public void generateEncounters(final int count, final BlackBox<Patient> patientBlackBox,
			final BlackBox<Diagnosis> diagnosisBlackBox, final BlackBox<Medication> medicationBlackBox,
			final BlackBox<Claim> claimBlackBox, final BlackBox<ClaimCharges> claimChargesBlackBox,
			final BlackBox<ClaimDetails> claimDetailsBlackBox, final BlackBox<Encounter> encounterBlackBox)
			throws BlackBoxException, IOException {
		final Patient p = new Patient();

		final Random random = new Random();
		assert patientBlackBox != null;
		final List<Patient> pList = patientBlackBox.search(Collections.singletonList(p));
		final List<Encounter> encounters = new ArrayList<>();
		for (int j = 0; j < count; j++) {
			final int id = random.nextInt();
			final Patient currentPatient = pList.get(Math.abs(id % pList.size()));
			final Encounter enc = new Encounter(id + "E", currentPatient);
			enc.setPatient(currentPatient);
			enc.setPatientFirstName(currentPatient.getFirstName());
			enc.setPatientLastName(currentPatient.getLastName());
			// Add random # of diagnosis
			int dCount = random.nextInt(2) + 1;
			for (int i = 0; i < dCount; i++) {
				final Diagnosis diagnosis = new Diagnosis(
						MedicalUseCaseTest.Diagnosis.get(random.nextInt(10000) % MedicalUseCaseTest.Diagnosis.size()));
				if (diagnosisBlackBox.update(diagnosis)) {
					enc.addDiagnosis(diagnosis);
				}

			}
			// Add random Medications
			dCount = random.nextInt(2) + 1;
			for (int i = 0; i < dCount; i++) {
				final Medication medication = new Medication(
						MedicalUseCaseTest.Meds.get(random.nextInt(10000) % MedicalUseCaseTest.Meds.size()));
				medication.setmDose(random.nextInt(8));
				if (medicationBlackBox.update(medication)) {
					enc.addMedication(medication);
				}
			}
			// add random # of claims
			dCount = random.nextInt(2) + 1;
			for (int i = 0; i < dCount; i++) {
				final Claim claim = new Claim(random.nextInt() + "CL", currentPatient.getPatientId());
				// generate random # of details and charges
				int cdCount = random.nextInt(5) + 1;
				for (int k = 0; k < cdCount; k++) {
					final ClaimDetails cDet = new ClaimDetails(random.nextInt() + "CD");
					if (claimDetailsBlackBox.update(cDet)) {
						claim.getcDets().add(cDet);
					}
				}
				cdCount = random.nextInt(5) + 1;
				for (int k = 0; k < cdCount; k++) {
					final ClaimCharges cc = new ClaimCharges(random.nextInt() + "CC", random.nextInt(10000) * 1.1);
					if (claimChargesBlackBox.update(cc)) {
						claim.getcCharges().add(cc);
					}
				}
				if (claimBlackBox.update(claim)) {
					enc.addClaim(claim);
				}
			}
			encounters.add(enc);
		}
		encounterBlackBox.update(encounters);
	}

	/**
	 * @param count
	 * @throws BlackBoxException
	 * @throws IOException
	 */
	public void generatePatients(final int count, final BlackBox<Patient> blackBox)
			throws BlackBoxException, IOException {
		final int knownPatientId = 123456;
		final List<Patient> patientList = new ArrayList<>();

		final Random random = new Random();
		for (int i = 0; i < count; i++) {
			final int id = random.nextInt();
			final Patient patient = new Patient(id + "P", FirstNames.get(Math.abs(id % FirstNames.size())),
					LastNames.get(Math.abs(random.nextInt() % LastNames.size())));
			patient.initData();
			patientList.add(patient);
		}
		// update the table
		assert blackBox != null;
		blackBox.update(patientList);

		// Create some patients with specific ids so that we can query them
		final Patient p = new Patient(knownPatientId + "P",
				FirstNames.get(Math.abs(knownPatientId % FirstNames.size())),
				LastNames.get(Math.abs(knownPatientId % LastNames.size())));
		p.initData();
		blackBox.update(Collections.singletonList(p));

	}

}
