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
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.beans.Customer;
import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.usecase.medical.MedicalUseCaseTest;
import com.dydabo.blackbox.usecase.medical.db.*;

import java.io.IOException;
import java.util.*;

/**
 * @author viswadas leher <vleher@gmail.com>
 */
public class DyDaBoTestUtils {

    /**
     *
     */
    public final List<String> FirstNames = Arrays.asList("David", "Peter", "Tom", "Dick", "Harry", "John", "Bill", "Adele", "Britney",
            "Mariah");

    /**
     *
     */
    public final List<String> LastNames = Arrays.asList("Johnson", "Becker", "Smith", "Gates", "King", "Spears", "Perry", "Carey",
            "Gomez", "Lopez");

    /**
     * @param maxNumber
     * @return
     */
    public List<Customer> generateCustomers(int maxNumber) {
        List<Customer> custList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < maxNumber; i++) {
            int id = Math.abs(random.nextInt());
            final Customer customer = new Customer(id, FirstNames.get(Math.abs(id % FirstNames.size())) + " "
                    + LastNames.get(Math.abs(id % LastNames.size())));
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
    public List<Employee> generateEmployees(int maxNumber) {
        List<Employee> userList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < maxNumber; i++) {
            int id = Math.abs(random.nextInt());
            final Employee employee = new Employee(id, FirstNames.get(Math.abs(id % FirstNames.size())) + " "
                    + LastNames.get(Math.abs(id % LastNames.size())));
            userList.add(employee);
        }
        return userList;
    }

    /**
     * @param count
     * @throws BlackBoxException
     * @throws IOException
     */
    public void generateEncounters(int count) throws BlackBoxException, IOException {
        Patient p = new Patient();
        BlackBox hbaseBlackBox = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
        BlackBox cassBlackBox = BlackBoxFactory.getDatabase(BlackBoxFactory.CASSANDRA);
        Random random = new Random();
        List<Patient> pList = hbaseBlackBox.search(Collections.singletonList(p));
        List<Encounter> encounters = new ArrayList<>();
        for (int j = 0; j < count; j++) {
            int id = random.nextInt();
            final Patient currentPatient = pList.get(Math.abs(id % pList.size()));
            Encounter enc = new Encounter(id + "E", currentPatient);
            enc.setPatient(currentPatient);
            enc.setpFN(currentPatient.getfN());
            enc.setpLN(currentPatient.getlN());
            // Add random # of diagnosis
            int dCount = random.nextInt(2) + 1;
            for (int i = 0; i < dCount; i++) {
                final Diagnosis diagnosis = new Diagnosis(
                        MedicalUseCaseTest.Diagnosis.get(random.nextInt(10000) % MedicalUseCaseTest.Diagnosis.size()));
                cassBlackBox.update(diagnosis);
                if (hbaseBlackBox.update(diagnosis)) {
                    enc.addDiagnosis(diagnosis);
                }

            }
            // Add random Medications
            dCount = random.nextInt(2) + 1;
            for (int i = 0; i < dCount; i++) {
                final Medication medication = new Medication(
                        MedicalUseCaseTest.Meds.get(random.nextInt(10000) % MedicalUseCaseTest.Meds.size()));
                medication.setmDose(random.nextInt(8));
                cassBlackBox.update(medication);
                if (hbaseBlackBox.update(medication)) {
                    enc.addMedication(medication);
                }
            }
            // add random # of claims
            dCount = random.nextInt(2) + 1;
            for (int i = 0; i < dCount; i++) {
                final Claim claim = new Claim(random.nextInt() + "CL", currentPatient.getpId());
                // generate random # of details and charges
                int cdCount = random.nextInt(5) + 1;
                for (int k = 0; k < cdCount; k++) {
                    ClaimDetails cDet = new ClaimDetails(random.nextInt() + "CD");
                    cassBlackBox.update(cDet);
                    if (hbaseBlackBox.update(cDet)) {
                        claim.getcDets().add(cDet);
                    }
                }
                cdCount = random.nextInt(5) + 1;
                for (int k = 0; k < cdCount; k++) {
                    ClaimCharges cc = new ClaimCharges(random.nextInt() + "CC", random.nextInt(10000) * 1.1);
                    cassBlackBox.update(cc);
                    if (hbaseBlackBox.update(cc)) {
                        claim.getcCharges().add(cc);
                    }
                }
                cassBlackBox.update(claim);
                if (hbaseBlackBox.update(claim)) {
                    enc.addClaim(claim);
                }
            }
            encounters.add(enc);
        }
        cassBlackBox.update(encounters);
        hbaseBlackBox.update(encounters);
    }

    /**
     * @param count
     * @throws BlackBoxException
     * @throws IOException
     */
    public void generatePatients(int count) throws BlackBoxException, IOException {
        int knownPatientId = 123456;
        List<Patient> patientList = new ArrayList<>();
        BlackBox hbaseBlackBox = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
        BlackBox cassBlackBox = BlackBoxFactory.getDatabase(BlackBoxFactory.CASSANDRA);
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            int id = random.nextInt();
            Patient patient = new Patient(id + "P", FirstNames.get(Math.abs(id % FirstNames.size())),
                    LastNames.get(Math.abs(random.nextInt() % LastNames.size())));
            patient.initData();
            patientList.add(patient);
        }
        // update the table
        hbaseBlackBox.update(patientList);
        cassBlackBox.update(patientList);
        // Create some patients with specific ids so that we can query them
        Patient p = new Patient(knownPatientId + "P", FirstNames.get(Math.abs(knownPatientId % FirstNames.size())),
                LastNames.get(Math.abs(knownPatientId % LastNames.size())));
        p.initData();
        hbaseBlackBox.update(Collections.singletonList(p));
        cassBlackBox.update(p);
    }

}
