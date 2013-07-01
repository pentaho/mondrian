/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2005 Pentaho and others
// All Rights Reserved.
*/
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
