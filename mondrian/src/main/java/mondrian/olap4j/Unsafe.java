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


package mondrian.olap4j;

import mondrian.olap.QueryTiming;
import mondrian.spi.ProfileHandler;

import org.olap4j.OlapStatement;

import java.io.PrintWriter;

/**
 * Access to non-public methods in the package of the mondrian olap4j driver.
 *
 * <p>All methods in this class are subject to change without notice.
 *
 * @author jhyde
 * @since October, 2010
 */
public final class Unsafe {
    public static final Unsafe INSTANCE = new Unsafe();

    private Unsafe() {
    }

    public void setStatementProfiling(
        OlapStatement statement,
        final PrintWriter pw)
    {
        ((MondrianOlap4jStatement) statement).enableProfiling(
            new ProfileHandler() {
                public void explain(String plan, QueryTiming timing) {
                    pw.println(plan);
                    if (timing != null) {
                        pw.println(timing);
                    }
                }
            }
        );
    }
}

// End Unsafe.java
