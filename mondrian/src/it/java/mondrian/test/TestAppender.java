/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2021 Hitachi Vantara and others
// All Rights Reserved.
//
*/
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
