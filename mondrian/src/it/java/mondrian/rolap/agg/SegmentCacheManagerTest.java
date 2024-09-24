/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2020 Hitachi Vantara.
// All Rights Reserved.
*/

package mondrian.rolap.agg;

import junit.framework.TestCase;
import mondrian.olap.MondrianException;
import mondrian.olap.MondrianServer;
import mondrian.server.Execution;
import mondrian.server.Locus;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SegmentCacheManagerTest extends TestCase {

  @Mock private MondrianServer mondrianServer;
  private Locus locus = new Locus( new Execution( null, 0 ), "component", "message" );
  private ExecutorService executor = Executors.newFixedThreadPool( 15 );

  public void setUp() throws Exception {
    super.setUp();
    MockitoAnnotations.initMocks( this );
  }

  public void testCommandExecution() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch( 1 );
    SegmentCacheManager man = new SegmentCacheManager( mondrianServer );
    man.execute( new MockCommand( latch::countDown ) );

    latch.await( 2000, TimeUnit.MILLISECONDS );
    assertEquals( latch.getCount(), 0 );
  }

  public void testShutdownEndOfQueue() throws InterruptedException {
    BlockingQueue execResults = new ArrayBlockingQueue( 10 );
    SegmentCacheManager man = new SegmentCacheManager( mondrianServer );
    // add 10 commands to the exec queue
    executeNtimes( execResults, man, 10 );
    // then shut down
    executor.submit( man::shutdown );
    List<Object> results = new ArrayList<>();
    // collect the results.  All should have completed successfully with "done".
    for ( int i = 0; i < 10; i++ ) {
      Object val = execResults.poll( 200, TimeUnit.MILLISECONDS );
      assertEquals( "done", val );
      results.add( val );
    }
    assertEquals( 10, results.size() );

  }

  public void testShutdownMiddleOfQueue() throws InterruptedException {
    BlockingQueue<Object> execResults = new ArrayBlockingQueue( 20 );

    SegmentCacheManager man = new SegmentCacheManager( mondrianServer );
    // submit 2 commands for exec
    executeNtimes( execResults, man, 2 );
    // submit shutdown
    executor.submit( man::shutdown );
    // submit 18 commands post-shutdown
    executeNtimes( execResults, man, 18 );

    // gather results.  There should be 20 full results, with those following shutdown containing an exception.
    List<Object> results = new ArrayList<>();
    for ( int i = 0; i < 20; i++ ) {
      results.add( execResults.poll( 2000, TimeUnit.MILLISECONDS ) );
    }
    assertEquals( 20, results.size() );
    assertEquals( "done", results.get( 0 ) );
    assertTrue( results.get( 19 ) instanceof MondrianException );
  }

  private void executeNtimes( BlockingQueue<Object> queue, SegmentCacheManager man, int n ) {
    for ( int i = 0; i < n; i++ ) {
      executor.submit( () ->
      {
        try {
          putInQueue( queue, man.execute( new MockCommand( this::sleep ) ) );
        } catch ( RuntimeException re ) {
          putInQueue( queue, re );
        }

      } );
    }
  }

  private void putInQueue( BlockingQueue<Object> queue, Object object ) {
    try {
      queue.put( object );
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException( e );
    }
  }


  @SuppressWarnings( "java:S2925" ) // using sleep here to simulate long running command
  private void sleep() {
    try {
      Thread.sleep( 100 );
    } catch ( InterruptedException e ) {
      throw new IllegalStateException( e );
    }
  }


  private class MockCommand extends SegmentCacheManager.Command<Object> {
    private final Runnable runnable;

    MockCommand( Runnable runnable ) {
      this.runnable = runnable;
    }

    @Override public Locus getLocus() {
      return locus;
    }

    @Override public Object call() {
      runnable.run();
      return "done";
    }
  }

}
