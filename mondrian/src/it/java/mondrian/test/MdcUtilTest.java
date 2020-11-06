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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

import mondrian.rolap.BatchTestCase;
import mondrian.rolap.RolapUtil;

/**
 * Verifies that MDC context is passed between threads.
 * 
 * @author Benny
 */
public class MdcUtilTest extends BatchTestCase {

  private static Logger rolapUtilLogger = Logger.getLogger( mondrian.rolap.RolapUtil.class );

  public void testMdcContext() throws Exception {

    TestContext.instance().flushSchemaCache();
    
    MDC.put( "sessionName", "hello-world" );
    StringWriter writer = new StringWriter();
    WriterAppender appender = new WriterAppender();
    PatternLayout layout = new PatternLayout( "sessionName:%X{sessionName} %t %m" );
    appender.setLayout( layout );
    appender.setWriter( writer );
    
    rolapUtilLogger.addAppender( appender );
    Level oldLevel = rolapUtilLogger.getLevel();
    rolapUtilLogger.setLevel( Level.DEBUG );
    
    RolapUtil.MONITOR_LOGGER.addAppender( appender );
    Level oldMonitorLevel = RolapUtil.MONITOR_LOGGER.getLevel();
    RolapUtil.MONITOR_LOGGER.setLevel( Level.DEBUG );

    RolapUtil.MDX_LOGGER.addAppender( appender );
    Level oldMdxLevel = RolapUtil.MDX_LOGGER.getLevel();
    RolapUtil.MDX_LOGGER.setLevel( Level.DEBUG );

    
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
      System.out.println(log);
      
    } finally {
      rolapUtilLogger.removeAppender( appender );
      rolapUtilLogger.setLevel( oldLevel );
      
      RolapUtil.MONITOR_LOGGER.removeAppender( appender );
      RolapUtil.MONITOR_LOGGER.setLevel( oldMonitorLevel );
      
      RolapUtil.MDX_LOGGER.removeAppender( appender );
      RolapUtil.MDX_LOGGER.setLevel( oldMdxLevel );
    }
    
    // Mondrian uses another thread pool to execute SQL statements.  
    // Verify that sessionName is now present on the SQL log statements
    assertContains(log, "sessionName:hello-world mondrian.rolap.agg.SegmentCacheManager$sqlExecutor_");
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
