/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2002-2020 Hitachi Vantara..  All rights reserved.
 */

package mondrian.calc.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.ResourceLimitExceededException;
import mondrian.test.PropertySaver5;


public class ArrayTupleListTest {

  /**
  * Access properties via this object and their values will be reset.
  */
  protected final PropertySaver5 propSaver = new PropertySaver5();  
  
  @AfterEach
  public void afterEach() {
      propSaver.reset();
  }
  private Member member1 = mock( Member.class );
  private Member member2 = mock( Member.class );
  private ArrayTupleList list;

  @Test
  public void testGrowListBeyondInitialCapacity() {
    propSaver.set( MondrianProperties.instance().ResultLimit, 0 );
    list = new ArrayTupleList( 2, 10 );
    addMockTuplesToList( list, 50 );

    assertEquals( list.size(), 50 );
    for ( int i = 0; i < 50; i++ ) {
      assertEquals( list.get( i ).get( 0 ), member1 );
      assertEquals( list.get( i ).get( 1 ), member2 );
    }
  }

  @Test
  public void testAttemptToGrowBeyondResultLimit() {
    propSaver.set( MondrianProperties.instance().ResultLimit, 30 );
    list = new ArrayTupleList( 2, 10 );
    try {
      addMockTuplesToList( list, 32 );
      fail( "Expected exception." );
    } catch ( ResourceLimitExceededException e ) {
      assertTrue( e.getMessage().contains( "result (31) exceeded limit (30)" ),
    		  "Actual message:  " + e.getMessage() + " \ndid not match expected");
    }
  }

  private void addMockTuplesToList( ArrayTupleList list, int count ) {
    for ( int i = 0; i < count; i++ ) {
      list.addTuple( member1, member2 );
    }
  }
}
