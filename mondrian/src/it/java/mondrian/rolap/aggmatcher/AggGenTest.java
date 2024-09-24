/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.rolap.aggmatcher;

import mondrian.olap.*;
import mondrian.rolap.RolapConnection;
import mondrian.test.FoodMartTestCase;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.LogManager;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;


/**
 * Test if lookup columns are there after loading them in
 * AggGen#addCollapsedColumn(...).
 *
 * @author Sherman Wood
 */
public class AggGenTest extends FoodMartTestCase {

    public AggGenTest(String name) {
        super(name);
    }

    public void
        testCallingLoadColumnsInAddCollapsedColumnOrAddzSpecialCollapsedColumn()
        throws Exception
    {
        Logger logger = LogManager.getLogger(AggGen.class);
        StringWriter writer = new StringWriter();

        final Appender appender =
            Util.makeAppender("testMdcContext", writer, null);

        Util.addAppender(appender, logger, org.apache.logging.log4j.Level.DEBUG);

        MondrianProperties props = MondrianProperties.instance();
        // If run in Ant and with mondrian.jar, please comment out this line:
        propSaver.set(props.AggregateRules, "DefaultRules.xml");
        propSaver.set(props.UseAggregates, true);
        propSaver.set(props.ReadAggregates, true);
        propSaver.set(props.GenerateAggregateSql, true);

        final RolapConnection rolapConn = (RolapConnection) getConnection();
        Query query =
            rolapConn.parseQuery(
                "select {[Measures].[Count]} on columns from [HR]");
        rolapConn.execute(query);

        Util.removeAppender(appender, logger);

        final DataSource dataSource = rolapConn.getDataSource();
        Connection sqlConnection = null;
        try {
            sqlConnection = dataSource.getConnection();
            DatabaseMetaData dbmeta = sqlConnection.getMetaData();
            JdbcSchema jdbcSchema = JdbcSchema.makeDB(dataSource);
            final String catalogName = jdbcSchema.getCatalogName();
            final String schemaName = jdbcSchema.getSchemaName();

            String log = writer.toString();
            Pattern p = Pattern.compile(
                "DEBUG - Init: Column: [^:]+: `(\\w+)`.`(\\w+)`"
                + Util.nl
                + "WARN - Can not find column: \\2");
            Matcher m = p.matcher(log);

            while (m.find()) {
                ResultSet rs =
                    dbmeta.getColumns(
                        catalogName, schemaName, m.group(1), m.group(2));
                assertTrue(!rs.next());
            }
        } finally {
            if (sqlConnection != null) {
                try {
                    sqlConnection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

}

// End AggGenTest.java
