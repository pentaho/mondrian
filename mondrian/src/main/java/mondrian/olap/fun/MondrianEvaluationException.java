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


package mondrian.olap.fun;

/**
 * Thrown while evaluating a cell expression
 *
 * @author jhyde, 14 June, 2002
 */
public class MondrianEvaluationException extends RuntimeException {
    public MondrianEvaluationException() {
    }
    public MondrianEvaluationException(String s) {
        super(s);
    }
}

// End MondrianEvaluationException.java
