  /*
  // This software is subject to the terms of the Eclipse Public License v1.0
  // Agreement, available at the following URL:
  // http://www.eclipse.org/legal/epl-v10.html.
  // You must accept the terms of that agreement to use this software.
  //
  // Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
  */
  package mondrian.util;

  import static org.mockito.Mockito.mock;
  import static org.mockito.Mockito.never;
  import static org.mockito.Mockito.verify;

  import junit.framework.TestCase;
  import mondrian.olap.MondrianProperties;
  import mondrian.server.Execution;

  public class CancellationCheckerTest extends TestCase {
    private Execution excMock = mock(Execution.class);

    public void testCheckCancelOrTimeoutWithIntExecution() {
      int currentIteration = 10;
      MondrianProperties.instance().CheckCancelOrTimeoutInterval.set(1);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutWithLongExecution() {
      long currentIteration = 10L;
      MondrianProperties.instance().CheckCancelOrTimeoutInterval.set(1);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutLongMoreThanIntExecution() {
      long currentIteration = 2147483648L;
      MondrianProperties.instance().CheckCancelOrTimeoutInterval.set(1);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutMaxLongExecution() {
      long currentIteration = 9223372036854775807L;
      MondrianProperties.instance().CheckCancelOrTimeoutInterval.set(1);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutNoExecution_IntervalZero() {
      int currentIteration = 10;
      MondrianProperties.instance().CheckCancelOrTimeoutInterval.set(0);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock, never()).checkCancelOrTimeout();
    }

    public void testCheckCancelOrTimeoutNoExecutionEvenIntervalOddIteration() {
      int currentIteration = 3;
      MondrianProperties.instance().CheckCancelOrTimeoutInterval.set(10);
      CancellationChecker.checkCancelOrTimeout(currentIteration, excMock);
      verify(excMock, never()).checkCancelOrTimeout();
    }

  }

// End CancellationCheckerTest.java
