package com.wugui.datax.admin.tool.create;

import com.wugui.datax.admin.entity.JobDatasource;
import com.wugui.datax.admin.tool.query.BaseQueryTool;
import com.wugui.datax.admin.tool.query.QueryToolFactory;
import com.wugui.datax.admin.util.JdbcConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class HiveCreateTable {
    protected static final Logger logger = LoggerFactory.getLogger(HiveCreateTable.class);

    public static void createHiveTable(JobDatasource datasource, String tableName, String envKey) {

        BaseQueryTool queryTool = QueryToolFactory.getByDbType(datasource);
        List<String> columnNames = queryTool.getColumnNames(tableName, datasource.getDatasource());
        try {
            Class.forName(JdbcConstants.HIVE_DRIVER);
            Connection con = DriverManager.getConnection(JdbcConstants.HIVE_DEFAULT_DATABASE, JdbcConstants.HIVE_USER, JdbcConstants.HIVE_PASSWORD);
            Statement stmt = con.createStatement();
            String hiveTableName = String.format("ods_%s", tableName);
            //获取所有字段和字段comment
            List<String> columnList = columnNames
                    .stream()
                    .map(field -> {
                                String fieldComment = queryTool.getSQLQueryFieldsComment(tableName, field);
                                return String.format("`%s` string comment '%s'", field, fieldComment);
                            }
                    )
                    .collect(Collectors.toList());
            String columns = String.join(",", columnList);
            String envKeyFiled = "".equals(envKey) ? "" : "`env_key__` string comment 'env_key',";
            String createTableSql = String.format("create external table if not exists %s " +
                    "(%s" +
                    " %s," +
                    "extra_field__ string comment '扩展字段') " +
                    "partitioned by (dt string) " +
                    "stored as PARQUET", hiveTableName, envKeyFiled, columns);
            stmt.execute(createTableSql);
            con.close();
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("create hive table error "
                    + "the exception message is:" + e.getMessage());
        }
    }
}
