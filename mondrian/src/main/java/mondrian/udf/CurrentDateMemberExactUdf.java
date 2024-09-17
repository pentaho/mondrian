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

package mondrian.udf;

import mondrian.olap.type.*;
import mondrian.util.Format;

/**
 * User-defined function <code>CurrentDateMember</code>.  Arguments to the
 * function are as follows:
 *
 * <blockquote>
 * <code>
 * CurrentDateMember(&lt;Hierarchy&gt;, &lt;FormatString&gt;)
 * returns &lt;Member&gt;
 * </code>
 * </blockquote>
 *
 * The function returns the member from the specified hierarchy that matches
 * the current date, to the granularity specified by the &lt;FormatString&gt;.
 *
 * The format string conforms to the format string implemented by
 * {@link Format}.
 *
 * @author Zelaine Fong
 */
public class CurrentDateMemberExactUdf extends CurrentDateMemberUdf {

    public String getDescription() {
        return "Returns the exact member within the specified dimension "
            + "corresponding to the current date, in the format specified by "
            + "the format parameter. "
            + "If there is no such date, returns the NULL member. "
            + "Format strings are the same as used by the MDX Format function, "
            + "namely the Visual Basic format strings. "
            + "See http://www.apostate.com/programming/vb-format.html.";
    }

    public Type[] getParameterTypes() {
        return new Type[] {
            new HierarchyType(null, null),
            new StringType()
        };
    }

    public String[] getReservedWords() {
        return null;
    }
}

// End CurrentDateMemberExactUdf.java
