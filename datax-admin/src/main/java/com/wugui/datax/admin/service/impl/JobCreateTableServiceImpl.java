package com.wugui.datax.admin.service.impl;

import com.wugui.datax.admin.entity.JobDatasource;
import com.wugui.datax.admin.service.JobCreateTableService;
import com.wugui.datax.admin.service.JobDatasourceService;
import com.wugui.datax.admin.tool.create.HiveCreateTable;
import com.wugui.datax.admin.util.JdbcConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

@Service
public class JobCreateTableServiceImpl implements JobCreateTableService {
    @Autowired
    private JobDatasourceService jobDatasourceService;

    @Override
    public Boolean createHiveTable(Long datasourceId, String tableName, String envKey) throws SQLException {
        JobDatasource datasource = jobDatasourceService.getById(datasourceId);
        boolean success = false;
        if (JdbcConstants.MYSQL.equals(datasource.getDatasource())) {
            //创建hive表
            HiveCreateTable.createHiveTable(datasource, tableName, envKey);
            success = true;
        }
        return success;
    }
}
