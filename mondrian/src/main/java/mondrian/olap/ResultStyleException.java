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

package mondrian.olap;

import mondrian.calc.ResultStyle;

import java.util.List;

/**
 * Exception that indicates a compiler could not implement an expression in any
 * of the result styles requested by the client.
 *
 * @author Richard Emberson
 */
public class ResultStyleException extends MondrianException {
    public static ResultStyleException generate(
        List<ResultStyle> producer,
        List<ResultStyle> consumer)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("Producer expected ResultStyles: ");
        buf.append('{');
        for (int i = 0; i < producer.size(); i++) {
            if (i > 0) {
                buf.append(',');
            }
            buf.append(producer.get(i));
        }
        buf.append('}');
        buf.append(" but Consumer wanted: ");
        buf.append('{');
        for (int i = 0; i < consumer.size(); i++) {
            if (i > 0) {
                buf.append(',');
            }
            buf.append(consumer.get(i));
        }
        buf.append('}');
        throw new ResultStyleException(buf.toString());
    }

    public static ResultStyleException generateBadType(
        List<ResultStyle> wanted,
        ResultStyle got)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("Wanted ResultStyles: ");
        buf.append('{');
        for (int i = 0; i < wanted.size(); i++) {
            if (i > 0) {
                buf.append(',');
            }
            buf.append(wanted.get(i));
        }
        buf.append('}');
        buf.append(" but got: ");
        buf.append(got);
        return new ResultStyleException(buf.toString());
    }

    public ResultStyleException(String message) {
        super(message);
    }
}

// End ResultStyleException.java
