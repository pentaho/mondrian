/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2020 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.test;

import java.io.StringWriter;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;

import mondrian.olap.Util;
import mondrian.rolap.BatchTestCase;
import mondrian.rolap.RolapUtil;

/**
 * Verifies that MDC context is passed between threads.
 * 
 * @author Benny
 */
public class MdcUtilTest extends BatchTestCase {

  private static Logger rolapUtilLogger = LogManager.getLogger( mondrian.rolap.RolapUtil.class );

  public void testMdcContext() throws Exception {

    TestContext.instance().flushSchemaCache();

    ThreadContext.put( "sessionName", "hello-world" );
    StringWriter writer = new StringWriter();

    final Appender appender =
        Util.makeAppender(
            "testMdcContext",
            writer,
            "sessionName:%X{sessionName} %t %m");

    Util.addAppender(appender, rolapUtilLogger, Level.ALL);
    Util.addAppender(appender, RolapUtil.MONITOR_LOGGER, Level.ALL);
    Util.addAppender(appender, RolapUtil.MDX_LOGGER, Level.ALL);

    String log = "";
    try {
      
      String query =
          "SELECT {[Measures].[Unit Sales]} " + "on columns, {[Gender].Members} on rows FROM [Sales]";
  
      String expected =
          "Axis #0:\n" + "{}\n" + "Axis #1:\n" + "{[Measures].[Unit Sales]}\n" + "Axis #2:\n"
              + "{[Gender].[All Gender]}\n" + "{[Gender].[F]}\n" + "{[Gender].[M]}\n" + "Row #0: 266,773\n"
              + "Row #1: 131,558\n" + "Row #2: 135,215\n";
  
      assertQueryReturns( query, expected );
      log = writer.toString();

    } finally {
      Util.removeAppender(appender, rolapUtilLogger);
      Util.removeAppender(appender, RolapUtil.MONITOR_LOGGER);
      Util.removeAppender(appender, RolapUtil.MDX_LOGGER);
    }
    
    // Mondrian uses another thread pool to execute SQL statements.  
    // Verify that sessionName is now present on the SQL log statements
    assertContains(log, "sessionName:hello-world mondrian.rolap.RolapResultShepherd$executor_1 18: SqlTupleReader.readTuples");
    assertContains(log, "sessionName:hello-world Mondrian Monitor StatementStartEvent");
    assertContains(log, "sessionName:hello-world mondrian.rolap.RolapResultShepherd$executor_");
  }
  
  public void assertContains( String actual, String contains ) {
    if ( actual.contains( contains ) ) {
      return;
    }
    fail( "Test String does not contain: " + contains + " Test String: " + actual );
  }

}
// End MdcUtilTest.java
