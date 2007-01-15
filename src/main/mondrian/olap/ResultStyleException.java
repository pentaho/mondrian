/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.calc.ExpCompiler.ResultStyle;
/**
 * Exception which indicates some resource limit was exceeded.
 *
 * @version $Id$
 */
public class ResultStyleException extends MondrianException {
    public static ResultStyleException generate(ResultStyle[] producer,
            ResultStyle[] consumer) {
        StringBuffer buf = new StringBuffer();
        buf.append("Producer expected ResultStyles: ");
        buf.append('{');
        for (int i = 0; i < producer.length; i++) {
            if (i > 0) {
                buf.append(',');
            }
            buf.append(producer[i]);
        }
        buf.append('}');
        buf.append(" but Consumer wanted: ");
        buf.append('{');
        for (int i = 0; i < consumer.length; i++) {
            if (i > 0) {
                buf.append(',');
            }
            buf.append(consumer[i]);
        }
        buf.append('}');
        throw new ResultStyleException(buf.toString());
    }
    public static ResultStyleException generateBadType(ResultStyle[] wanted,
            ResultStyle got) {
        StringBuffer buf = new StringBuffer();
        buf.append("Wanted ResultStyles: ");
        buf.append('{');
        for (int i = 0; i < wanted.length; i++) {
            if (i > 0) {
                buf.append(',');
            }
            buf.append(wanted[i]);
        }
        buf.append('}');
        buf.append(" but got: ");
        buf.append(got);
        throw new ResultStyleException(buf.toString());
    }

    public ResultStyleException(String message) {
        super(message);
    }
}

// End ResourceLimitExceeded.java
