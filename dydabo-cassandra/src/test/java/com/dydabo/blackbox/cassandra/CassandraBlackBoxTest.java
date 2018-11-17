package com.dydabo.blackbox.cassandra;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CassandraBlackBoxTest {

    private CassandraBlackBox<BlackBoxable> bb;
    private BlackBoxable row;

    @BeforeEach
    void setUp() {
        row = new Dummy();
        bb = new CassandraBlackBox<>();
    }

    @Test
    void delete() throws BlackBoxException {
        bb.delete(row);
    }

    @Test
    void fetch() throws BlackBoxException {
        bb.fetch(row);
    }

    @Test
    void fetchByPartialKey() throws BlackBoxException {
        bb.fetchByPartialKey(row);
    }

    @Test
    void insert() throws BlackBoxException {
        bb.insert(row);
    }

    @Test
    void search() throws BlackBoxException {
        bb.search(row);
    }

    @Test
    void rangeSearch() throws BlackBoxException {
        bb.search(row, row);
    }

    @Test
    void update() throws BlackBoxException {
        bb.update(row);
    }

    private class Dummy implements BlackBoxable {
        @Override
        public String getBBJson() {
            return null;
        }

        @Override
        public String getBBRowKey() {
            return null;
        }
    }
}
