package com.dydabo.blackbox.cassandra.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class CassandraUtilsTest {
    private CassandraUtils<BlackBoxable> utils;
    @Mock
    private CassandraConnectionManager connectionManager;
    @Mock
    private CqlSession session;
    @Mock
    private TableMetadata tableMetadata;
    @Mock
    private KeyspaceMetadata keyspaceMetadata;
    @Mock
    private Metadata metadata;

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
        doReturn(Optional.of(keyspaceMetadata)).when(metadata).getKeyspace(anyString());
        doReturn(metadata).when(session).getMetadata();
        doReturn(session).when(connectionManager).getSession();
        doReturn(mock(ResultSet.class)).when(session).execute(any(Statement.class));
        assertTrue(utils.createTable(row));
    }

    @ParameterizedTest
    @MethodSource("getRows")
    void createNonEmptyTable(BlackBoxable row) {
        doReturn(Optional.of(keyspaceMetadata)).when(metadata).getKeyspace(anyString());
        doReturn(metadata).when(session).getMetadata();
        doReturn(session).when(connectionManager).getSession();
        doReturn(Optional.empty()).when(keyspaceMetadata).getTable(anyString());
        doReturn(mock(ResultSet.class)).when(session).execute(any(Statement.class));
        assertTrue(utils.createTable(row));
    }

    @ParameterizedTest
    @MethodSource("getRowsWithCols")
    void createIndex(String columnName, BlackBoxable row) {
        doReturn(Optional.of(keyspaceMetadata)).when(metadata).getKeyspace(anyString());
        doReturn(metadata).when(session).getMetadata();
        doReturn(session).when(connectionManager).getSession();

        assertTrue(utils.createIndex(columnName, row));
    }

    private static class TestObject implements BlackBoxable {
        @Override
        public List<Optional<Object>> getBBRowKeys() {
            return Collections.emptyList();
        }
    }
}
