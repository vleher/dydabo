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
package com.dydabo.blackbox.blackbox.usecase.medical;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.hbase.HBaseBlackBoxImpl;
import com.dydabo.blackbox.usecase.medical.MedicalUseCaseTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author viswadas leher
 */
public class HBaseUseCaseTest extends MedicalUseCaseTest {

    /**
     * @throws BlackBoxException
     * @throws IOException
     */
    public HBaseUseCaseTest() throws BlackBoxException, IOException {
        super();
        if (utils.dbToTest.contains(BlackBoxFactory.Databases.HBASE)) {
            instance = new HBaseBlackBoxImpl();
            // Pre-populate with some dynamic data.
            utils.generatePatients(random.nextInt(98765) % 10, instance);
            utils.generateEncounters(random.nextInt(98765) % 10, instance);
        }
    }

    @Test
    public void testAllClaimChargesForPatient() throws BlackBoxException {
        if (instance != null) {
            super.testAllClaimChargesForPatient();
        }
    }

    @Test
    public void testAllClaimsForPatient() throws BlackBoxException {
        if (instance != null) {
            super.testAllClaimsForPatient();
        }
    }

    @Test
    public void testAllPatientEncounters() throws BlackBoxException {
        if (instance != null) {
            super.testAllPatientEncounters();
        }
    }

    @Test
    public void testAllPatients() throws BlackBoxException {
        if (instance != null) {
            super.testAllPatients();
        }
    }

    @Test
    public void testAllPatientsWithDiagnosis() throws BlackBoxException {
        if (instance != null) {
            super.testAllPatientsWithDiagnosis();
        }
    }

    @Test
    public void testFetchQueryPerformance() throws BlackBoxException {
        if (instance != null) {
            super.testFetchQueryPerformance();
        }
    }

    @Test
    public void testGetEncounterByPatient() throws BlackBoxException {
        if (instance != null) {
            super.testGetEncounterByPatient();
        }
    }

    @Test
    public void testPatientByName() throws BlackBoxException {
        if (instance != null) {
            super.testPatientByName();
        }
    }

    @Test
    public void testPatientEncountersById() throws BlackBoxException {
        if (instance != null) {
            super.testPatientEncountersById();
        }
    }

    @Test
    public void testPatientEncountersByMedId() throws BlackBoxException {
        if (instance != null) {
            super.testPatientEncountersByMedId();
        }
    }

    @Test
    public void testPatientEncountersByMedIdAndPFName() throws BlackBoxException {
        if (instance != null) {
            super.testPatientEncountersByMedIdAndPFName();
        }
    }

    @Test
    public void testPatientEncountersByName() throws BlackBoxException {
        if (instance != null) {
            super.testPatientEncountersByName();
        }
    }

    @Test
    public void testPatientWithId() throws BlackBoxException {
        if (instance != null) {
            super.testPatientWithId();
        }
    }

    @Test
    public void testPatientsWithFirstAndLastName() throws BlackBoxException {
        if (instance != null) {
            super.testPatientsWithFirstAndLastName();
        }
    }

    @Test
    public void testRangeSearch() throws BlackBoxException {
        if (instance != null) {
            super.testRangeSearch();
        }
    }

}
