/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.aggmatcher;

import java.io.StringWriter;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnection;
import mondrian.test.FoodMartTestCase;

import javax.sql.DataSource;


/**
 * Test if lookup columns are there after loading them in
 * AggGen#addCollapsedColumn(...).
 *
 * @author Sherman Wood
 * @version $Id$
 */
public class AggGenTest extends FoodMartTestCase {

    public AggGenTest(String name) {
        super(name);
    }

    public void testCallingLoadColumnsInAddCollapsedColumnOrAddzSpecialCollapsedColumn() throws Exception {
        Logger logger = Logger.getLogger(AggGen.class);
        StringWriter writer = new StringWriter();
        Appender myAppender = new WriterAppender(new SimpleLayout(), writer);
        logger.addAppender(myAppender);
        logger.setLevel(Level.DEBUG);

        final String trueValue = "true";

        // This modifies the MondrianProperties for the whole of the
        // test run

        MondrianProperties props = MondrianProperties.instance();
        props.AggregateRules.setString("DefaultRules.xml"); // If run in Ant and with mondrian.jar, please comment out this line
        props.UseAggregates.setString(trueValue);
        props.ReadAggregates.setString(trueValue);
        props.GenerateAggregateSql.setString(trueValue);

        final RolapConnection rolapConn = (RolapConnection) getConnection();
        Query query = rolapConn.parseQuery("select {[Measures].[Count]} on columns from [HR]");
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
