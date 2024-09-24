/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
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
