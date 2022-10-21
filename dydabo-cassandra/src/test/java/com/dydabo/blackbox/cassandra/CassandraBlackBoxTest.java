package com.dydabo.blackbox.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class CassandraBlackBoxTest {

    private CassandraBlackBox<BlackBoxable> bb;
    private BlackBoxable row;
    @Mock
    private CqlSession session;
    @Mock
    private CassandraConnectionManager connectionManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        row = new Dummy();
        bb = new CassandraBlackBox<>(connectionManager);
    }

    private void setupSession() {
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);

        doReturn(Optional.of(keyspaceMetadata)).when(metadata).getKeyspace(anyString());
        doReturn(Optional.of(tableMetadata)).when(keyspaceMetadata).getTable(anyString());
        doReturn(session).when(connectionManager).getSession();
        doReturn(metadata).when(session).getMetadata();
    }

    private void setupResultSet() {
        ResultSet resultSet = mock(ResultSet.class);
        doReturn(resultSet).when(session).execute(any(Statement.class));
        List<Row> rowList = Collections.emptyList();
        doReturn(rowList.iterator()).when(resultSet).iterator();
    }

    @Test
    void delete() throws BlackBoxException {
        setupSession();
        bb.delete(row);
    }

    @Test
    void fetch() throws BlackBoxException {
        setupSession();
        setupResultSet();
        bb.fetch(row);
    }

    @Test
    void fetchByPartialKey() throws BlackBoxException {
        doReturn(session).when(connectionManager).getSession();
        setupResultSet();

        bb.fetchByPartialKey(row);
    }

    @Test
    void insert() throws BlackBoxException {
        setupSession();
        ResultSet resultSet = mock(ResultSet.class);
        doReturn(resultSet).when(session).execute(any(Statement.class));
        bb.insert(row);
    }

    @Test
    void search() throws BlackBoxException {
        setupSession();
        setupResultSet();

        bb.search(row);
    }

    @Test
    void rangeSearch() throws BlackBoxException {
        setupSession();
        setupResultSet();
        bb.search(row, row);
    }

    @Test
    void update() throws BlackBoxException {
        setupSession();
        ResultSet resultSet = mock(ResultSet.class);
        doReturn(resultSet).when(session).execute(any(Statement.class));
        bb.update(row);
    }

    private class Dummy implements BlackBoxable {
        @Override
        public List<Optional<Object>> getBBRowKeys() {
            return Collections.emptyList();
        }
    }
}
