/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2021 Hitachi Vantara..  All rights reserved.
*/

package mondrian.olap;

import mondrian.util.ArrayStack;

import java.util.*;

/**
 * Provides hooks for recording timing information of components of Query execution.
 *
 * <p>
 * NOTE: This class is experimental and subject to change/removal without notice.
 *
 * <p>
 * Code that executes as part of a Query can call {@link QueryTiming#markStart(String)} before executing, and
 * {@link QueryTiming#markEnd(String)} afterwards, or can track execution times manually and call
 * {@link QueryTiming#markFull(String, long)}.
 *
 * <p>
 * To read timing information, add a handler to the statement using {@link mondrian.server.Statement#enableProfiling}
 * and implement the {@link mondrian.spi.ProfileHandler#explain(String, QueryTiming)} method.
 *
 * @author jbarnett
 */
public class QueryTiming {
  private boolean enabled;
  private final ArrayStack<TimingInfo> currentTimings = new ArrayStack<>();
  // Tracks Query components that are already on the stack so that we don't double count their durations
  private final HashMap<String, Integer> currentTimingDepth = new HashMap<>();
  private final Map<String, DurationCount> timings = new HashMap<>();
  private final Map<String, DurationCount> fullTimings = new HashMap<>();

  /**
   * Initializes (or re-initializes) a query timing, also setting whether enabled. All previous stats are removed.
   *
   * @param enabled
   *          Whether to collect stats in future
   */
  public void init( boolean enabled ) {
    this.enabled = enabled;
    currentTimings.clear();
    timings.clear();
    fullTimings.clear();
  }

  public void done() {
  }

  /**
   * Marks the start of a Query component's execution.
   *
   * @param name
   *          Name of the component
   */
  public final void markStart( String name ) {
    if ( enabled ) {
      markStartInternal( name );
    }
  }

  /**
   * Marks the end of a Query component's execution.
   *
   * @param name
   *          Name of the component
   */
  public final void markEnd( String name ) {
    if ( enabled ) {
      long tstamp = System.currentTimeMillis();
      markEndInternal( name, tstamp );
    }
  }

  /**
   * Marks the duration of a Query component's execution.
   * 
   * markFull is synchronized because it may be called from either Actor's spawn thread or RolapResultShepherd thread
   * 
   * @param name
   *          Name of the component
   * @param duration
   *          Duration of the execution
   */
  public synchronized final void markFull( String name, long duration ) {
    if ( enabled ) {
      markFullInternal( name, duration );
    }
  }

  private void markStartInternal( String name ) {
    currentTimings.push( new TimingInfo( name ) );
    Integer depth = currentTimingDepth.get( name );
    if ( depth == null ) {
      currentTimingDepth.put( name, 1 );
    } else {
      currentTimingDepth.put( name, depth + 1 );
    }
  }

  private void markEndInternal( String name, long tstamp ) {
    if ( currentTimings.isEmpty() || !currentTimings.peek().name.equals( name ) ) {
      throw new IllegalStateException( "end but no start for " + name );
    }
    TimingInfo finished = currentTimings.pop();
    assert finished.name.equals( name );
    finished.markEnd( tstamp );

    DurationCount dc = timings.get( finished.name );
    if ( dc == null ) {
      dc = new DurationCount();
      timings.put( finished.name, dc );
    }
    dc.count++;
    Integer depth = currentTimingDepth.get( name );
    if ( depth == 1 ) {
      dc.duration += ( finished.endTime - finished.startTime );
    }
    currentTimingDepth.put( name, depth - 1 );
    
  }

  private void markFullInternal( String name, long duration ) {
    DurationCount p = fullTimings.get( name );
    if ( p == null ) {
      p = new DurationCount();
      fullTimings.put( name, p );
    }
    p.count++;
    p.duration += duration;
  }

  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append( "Query Timing (Cumulative):" );
    for ( Map.Entry<String, DurationCount> entry : timings.entrySet() ) {
      sb.append( Util.nl );
      sb.append( entry.getKey() ).append( " invoked " ).append( entry.getValue().count ).append(
          " times for total of " ).append( entry.getValue().duration ).append( "ms.  (Avg. " ).append( entry
              .getValue().duration / entry.getValue().count ).append( "ms/invocation)" );
    }
    for ( Map.Entry<String, DurationCount> entry : fullTimings.entrySet() ) {
      if ( sb.length() > 0 ) {
        sb.append( Util.nl );
      }
      sb.append( entry.getKey() ).append( " invoked " ).append( entry.getValue().count ).append(
          " times for total of " ).append( entry.getValue().duration ).append( "ms.  (Avg. " ).append( entry
              .getValue().duration / entry.getValue().count ).append( "ms/invocation)" );
    }
    sb.append( Util.nl );
    return sb.toString();
  }

  private static class TimingInfo {
    private final String name;
    private final long startTime;
    private long endTime;

    private TimingInfo( String name ) {
      this.name = name;
      this.startTime = System.currentTimeMillis();
    }

    private void markEnd( long tstamp ) {
      this.endTime = tstamp;
    }
  }

  private static class DurationCount {
    long duration;
    long count;
  }
}

// End QueryTiming.java
