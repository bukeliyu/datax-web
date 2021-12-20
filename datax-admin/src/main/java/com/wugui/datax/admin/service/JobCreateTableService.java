package com.wugui.datax.admin.service;

import java.sql.SQLException;

public interface JobCreateTableService {

    /**
     * 根据mysql表创建对应hive表
     * @param datasourceId
     * @param tableName
     * @param envKey
     * @return
     * @throws SQLException
     */
    Boolean createHiveTable(Long datasourceId, String tableName, String envKey) throws SQLException;
}
