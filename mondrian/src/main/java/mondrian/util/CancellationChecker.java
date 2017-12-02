/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2016-2016 Pentaho and others
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.MondrianProperties;
import mondrian.server.Execution;

/**
 * Encapsulates cancel and timeouts checks
 *
 * @author Yury_Bakhmutski
 * @since Jan 18, 2016
 */
public class CancellationChecker {

  public static void checkCancelOrTimeout(
      int currentIteration, Execution execution)
  {
    checkCancelOrTimeout((long) currentIteration, execution);
  }

  public static void checkCancelOrTimeout(
      long currentIteration, Execution execution)
  {
    int checkCancelOrTimeoutInterval = MondrianProperties.instance()
        .CheckCancelOrTimeoutInterval.get();
    if (execution != null) {
      synchronized (execution) {
        if (checkCancelOrTimeoutInterval > 0
            && currentIteration % checkCancelOrTimeoutInterval == 0)
        {
          execution.checkCancelOrTimeout();
        }
      }
    }
  }
}
// End CancellationChecker.java
