package com.dydabo.blackbox.common;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * @author viswadas leher
 */
public abstract class AbstractBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {

    private ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

    public ForkJoinPool getForkJoinPool() {
        return forkJoinPool;
    }

    public boolean delete(T row) throws BlackBoxException {
        return delete(Collections.singletonList(row));
    }

    @Override
    public List<T> fetch(String rowKey, T bean) throws BlackBoxException {
        return fetch(Collections.singletonList(rowKey), bean);
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean) throws BlackBoxException {
        return fetchByPartialKey(rowKeys, bean, -1);
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean) throws BlackBoxException {
        return fetchByPartialKey(rowKey, bean, -1);
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean, long maxResults) throws BlackBoxException {
        return fetchByPartialKey(Collections.singletonList(rowKey), bean, -1);
    }

    @Override
    public boolean insert(T row) throws BlackBoxException {
        return insert(Collections.singletonList(row));
    }

    @Override
    public List<T> search(List<T> rows) throws BlackBoxException {
        return search(rows, -1);
    }

    @Override
    public List<T> search(T row) throws BlackBoxException {
        return search(row, -1);
    }

    @Override
    public List<T> search(T row, long maxResults) throws BlackBoxException {
        return search(Collections.singletonList(row), maxResults);
    }

    @Override
    public List<T> search(T startRow, T endRow) throws BlackBoxException {
        return search(startRow, endRow, -1);
    }

    @Override
    public boolean update(T newRow) throws BlackBoxException {
        return update(Collections.singletonList(newRow));
    }
}
