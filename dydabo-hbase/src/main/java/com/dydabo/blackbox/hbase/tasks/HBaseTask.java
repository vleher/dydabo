package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * @author viswadas leher
 */
public abstract class HBaseTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {
    protected final Connection connection;
    protected HBaseUtils<T> utils;

    public HBaseTask(Connection connection) {
        this.utils = new HBaseUtils<>();
        this.connection = connection;
    }

    protected HBaseUtils<T> getUtils() {
        if (utils == null) {
            utils = new HBaseUtils<>();
        }
        return utils;
    }

    /**
     * @return
     */
    protected Connection getConnection() {
        return connection;
    }
}
