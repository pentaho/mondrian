package mondrian.spi.impl;

import mondrian.server.Execution;
import mondrian.spi.Dialect;
import mondrian.spi.impl.SqlStatisticsProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mondrian.rolap.RolapUtil;
import mondrian.rolap.SqlStatement;
import mondrian.server.Locus;

import java.sql.*;
import java.util.Arrays;
import javax.sql.DataSource;


/**
 * Created by Giovanny on 25/06/2016.
 */
public class PostgresqlSqlStatisticsProvider extends SqlStatisticsProvider {

    private static final Log LOGGER = LogFactory.getLog(PostgresqlSqlStatisticsProvider.class);

    public PostgresqlSqlStatisticsProvider () {
        LOGGER.info("PostgresqlSqlStatisticsProvider creado");
    }

    protected int  executeStament (SqlStatement stmt) {
        int count = -1;
        try {
            ResultSet resultSet = stmt.getResultSet();
            if (resultSet.next()) {
                count = resultSet.getInt(1);
            }

        } catch (Exception e) {

        } finally {
            stmt.close();
        }
        return  count;
    }

    @Override
    public int getTableCardinality(Dialect dialect, DataSource dataSource, String catalog, String schema, String table, Execution execution) {

        StringBuilder buf = new StringBuilder("SELECT sum(reltuples::bigint) AS count FROM pg_class WHERE oid in (\n")
               .append("SELECT\n" )
               .append("    (nmsp_child.nspname  || '.' ||    child.relname)::regclass\n" )
               .append("FROM pg_inherits\n" )
               .append("    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid\n" )
               .append("    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid\n" )
               .append("    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace\n" )
               .append("    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace\n" )
               .append("WHERE parent.relname='").append(table).append("');");

        String sql = buf.toString();

        SqlStatement stmt = RolapUtil.executeQuery(dataSource,sql,
                        new Locus(execution, "SqlStatisticsProvider.getTableCardinality","Reading row count from table " + Arrays.asList(catalog, schema, table)));

        int count = executeStament(stmt);

        if (count <= 0) {
            buf = new StringBuilder("SELECT reltuples AS count FROM pg_class WHERE relname = '").append(table).append("';");
            sql = buf.toString();
            stmt = RolapUtil.executeQuery(dataSource,sql,
                    new Locus(execution, "SqlStatisticsProvider.getTableCardinality","Reading row count from table " + Arrays.asList(catalog, schema, table)));

            count = executeStament(stmt);

        }

        if (count >= 0) {
            return count;
        }

        return super.getTableCardinality(dialect, dataSource, catalog, schema, table, execution);
    }
}
