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

  package mondrian.util;

  import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import mondrian.server.Execution;
import mondrian.test.PropertyRestoringTestCase;

  public class CancellationCheckerTest extends PropertyRestoringTestCase {
    private Execution excMock = mock(Execution.class);

    public void testCheckCancelOrTimeoutWithIntExecution() {
      int currentIteration = 10;
      propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 1);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutWithLongExecution() {
      long currentIteration = 10L;
      propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 1);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutLongMoreThanIntExecution() {
      long currentIteration = 2147483648L;
      propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 1);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutMaxLongExecution() {
      long currentIteration = 9223372036854775807L;
      propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 1);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutNoExecution_IntervalZero() {
      int currentIteration = 10;
      propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 0);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock, never()).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutNoExecutionEvenIntervalOddIteration() {
      int currentIteration = 3;
      propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 10);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock, never()).checkCancelOrTimeout();
    }

  }

// End CancellationCheckerTest.java
