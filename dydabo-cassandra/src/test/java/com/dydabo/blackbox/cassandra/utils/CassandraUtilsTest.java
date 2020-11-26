package com.dydabo.blackbox.cassandra.utils;

import com.datastax.driver.core.*;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class CassandraUtilsTest {
    private CassandraUtils<BlackBoxable> utils;
    @Mock
    private CassandraConnectionManager connectionManager;
    @Mock
    private Session session;
    @Mock
    private TableMetadata tableMetadata;
    @Mock
    private KeyspaceMetadata keyspaceMetadata;

    private static Stream<BlackBoxable> getRows() {
        BlackBoxable emptyRow = new TestObject();
        return Stream.of(emptyRow);
    }

    private static Stream<Arguments> getRowsWithCols() {
        BlackBoxable emptyRow = new TestObject();
        return Stream.of(Arguments.of("", emptyRow));
    }

    private static Stream<Arguments> getTableNamesRows() {
        BlackBoxable emptyRow = new TestObject();
        return Stream.of(Arguments.of(emptyRow, "TestObject"));
    }

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        Metadata metadata = mock(Metadata.class);
        Cluster cluster = mock(Cluster.class);

        doReturn(cluster).when(connectionManager).getCluster();
        doReturn(metadata).when(cluster).getMetadata();
        doReturn(keyspaceMetadata).when(metadata).getKeyspace(anyString());
        doReturn(tableMetadata).when(keyspaceMetadata).getTable(anyString());
        doReturn(session).when(connectionManager).getSession();

        utils = new CassandraUtils<>(connectionManager);
    }

    @ParameterizedTest
    @MethodSource("getTableNamesRows")
    void getTableName(BlackBoxable row, String result) {
        assertEquals(result, utils.getTableName(row));
    }

    @ParameterizedTest
    @MethodSource("getRows")
    void createEmptyTable(BlackBoxable row) {
        assertTrue(utils.createTable(row));
    }

    @ParameterizedTest
    @MethodSource("getRows")
    void createNonEmptyTable(BlackBoxable row) {
        doReturn(null).when(keyspaceMetadata).getTable(anyString());
        assertTrue(utils.createTable(row));
    }

    @ParameterizedTest
    @MethodSource("getRowsWithCols")
    void createIndex(String columnName, BlackBoxable row) {
        assertTrue(utils.createIndex(columnName, row));
    }


    private static class TestObject implements BlackBoxable {
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
