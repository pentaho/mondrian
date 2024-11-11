/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * TestAppender captures log4j events for unit testing.
 * 
 * @author benny
 */
public class TestAppender extends AbstractAppender {
  private final List<LogEvent> logEvents = new ArrayList<>();

  public TestAppender() {
    super( "TestAppender", null, PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY );
  }

  @Override
  public void append( final LogEvent loggingEvent ) {
    logEvents.add( loggingEvent.toImmutable() );
  }

  public List<LogEvent> getLogEvents() {
    return logEvents;
  }

  public void clear() {
    logEvents.clear();
  }
}

// End TestAppender.java
