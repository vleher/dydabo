/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
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
package com.dydabo.blackbox.hbase.usecase.medical;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.hbase.HBaseBlackBox;
import com.dydabo.test.blackbox.BlackBoxFactory;
import com.dydabo.test.blackbox.usecase.medical.MedicalUseCaseTest;

/**
 * @author viswadas leher
 */
public class HBaseUseCaseTest extends MedicalUseCaseTest {

	/**
	 * @throws BlackBoxException
	 * @throws IOException
	 */
	public HBaseUseCaseTest() throws BlackBoxException, IOException {
		if (utils.dbToTest.contains(BlackBoxFactory.Databases.HBASE)) {
			patientBlackBox = new HBaseBlackBox<>();
			encounterBlackBox = new HBaseBlackBox<>();
			medicationBlackBox = new HBaseBlackBox<>();
			claimChargesBlackBox = new HBaseBlackBox<>();
			claimBlackBox = new HBaseBlackBox<>();
			// Pre-populate with some dynamic data.
			utils.generatePatients(random.nextInt(98765) % 10, patientBlackBox);
			utils.generateEncounters(random.nextInt(98765) % 10, patientBlackBox, new HBaseBlackBox<>(),
					medicationBlackBox, claimBlackBox, claimChargesBlackBox, new HBaseBlackBox<>(), encounterBlackBox);
		}
	}

	@Override
	@Test
	public void testAllClaimChargesForPatient() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testAllClaimChargesForPatient();
		}
	}

	@Override
	@Test
	public void testAllClaimsForPatient() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testAllClaimsForPatient();
		}
	}

	@Override
	@Test
	public void testAllPatientEncounters() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testAllPatientEncounters();
		}
	}

	@Override
	@Test
	public void testAllPatients() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testAllPatients();
		}
	}

	@Override
	@Test
	public void testAllPatientsWithDiagnosis() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testAllPatientsWithDiagnosis();
		}
	}

	@Override
	@Test
	public void testFetchQueryPerformance() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testFetchQueryPerformance();
		}
	}

	@Override
	@Test
	public void testGetEncounterByPatient() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testGetEncounterByPatient();
		}
	}

	@Override
	@Test
	public void testPatientByName() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testPatientByName();
		}
	}

	@Override
	@Test
	public void testPatientEncountersById() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testPatientEncountersById();
		}
	}

	@Override
	@Test
	public void testPatientEncountersByMedId() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testPatientEncountersByMedId();
		}
	}

	@Override
	@Test
	public void testPatientEncountersByMedIdAndPFName() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testPatientEncountersByMedIdAndPFName();
		}
	}

	@Override
	@Test
	public void testPatientEncountersByName() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testPatientEncountersByName();
		}
	}

	@Override
	@Test
	public void testPatientWithId() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testPatientWithId();
		}
	}

	@Override
	@Test
	public void testPatientsWithFirstAndLastName() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testPatientsWithFirstAndLastName();
		}
	}

	@Override
	@Test
	public void testRangeSearch() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testRangeSearch();
		}
	}

	@Override
	@Test
	public void testMaxResultsSearch() throws BlackBoxException {
		if (patientBlackBox != null) {
			super.testMaxResultsSearch();
		}
	}

}
