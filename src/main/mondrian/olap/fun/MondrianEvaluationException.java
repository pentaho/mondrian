/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 14 June, 2002
*/
package mondrian.olap.fun;

/**
 * Thrown while evaluating a cell expression
 */
public class MondrianEvaluationException extends RuntimeException {
    public MondrianEvaluationException() {
    }
    public MondrianEvaluationException(String s) {
        super(s);
    }
}

// End MondrianEvaluationException.java
