/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.*;
import mondrian.rolap.RolapConnection;
import mondrian.test.FoodMartTestCase;

import org.apache.log4j.*;
import org.apache.log4j.Level;

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
        Logger logger = Logger.getLogger(AggGen.class);
        StringWriter writer = new StringWriter();
        Appender myAppender = new WriterAppender(new SimpleLayout(), writer);
        logger.addAppender(myAppender);
        propSaver.setAtLeast(logger, Level.DEBUG);

        // This modifies the MondrianProperties for the whole of the
        // test run

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

        logger.removeAppender(myAppender);

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
