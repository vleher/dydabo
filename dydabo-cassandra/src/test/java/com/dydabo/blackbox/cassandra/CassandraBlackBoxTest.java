package com.dydabo.blackbox.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Select;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class CassandraBlackBoxTest {

    private CassandraBlackBox<BlackBoxable> bb;
    private BlackBoxable row;
    @Mock
    private Session session;
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
        Cluster cluster = mock(Cluster.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);

        doReturn(cluster).when(connectionManager).getCluster();
        doReturn(metadata).when(cluster).getMetadata();
        doReturn(keyspaceMetadata).when(metadata).getKeyspace(anyString());
        doReturn(tableMetadata).when(keyspaceMetadata).getTable(anyString());
        doReturn(session).when(connectionManager).getSession();
    }

    @Test
    void delete() throws BlackBoxException {
        setupSession();
        bb.delete(row);
    }

    @Test
    void fetch() throws BlackBoxException {
        doReturn(session).when(connectionManager).getSession();
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
        bb.insert(row);
    }

    @Test
    void search() throws BlackBoxException {
        setupSession();
        setupResultSet();

        bb.search(row);
    }

    private void setupResultSet() {
        ResultSet resultSet = mock(ResultSet.class);
        doReturn(resultSet).when(session).execute(any(Select.class));
        List<Row> rowList = Collections.emptyList();
        doReturn(rowList.iterator()).when(resultSet).iterator();
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
