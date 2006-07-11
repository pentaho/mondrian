/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.udf;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.util.*;

/**
 * User-defined function CurrentDateMember.  Arguments to the function are
 * as follows:
 *
 * <code>
 * CurrentDataMember(<Hierarchy>, <FormatString>) returns <Member>
 * </code>
 *
 * The function returns the member from the specified hierarchy that matches
 * the current date, to the granularity specified by the <FormatString>.
 *
 * The format string conforms to the format string implemented by
 * {@link Format}.
 * 
 * @author Zelaine Fong
 */
public class CurrentDateMemberExactUdf extends CurrentDateMemberUdf {
    
    public String getDescription() {
        return "Returns the member corresponding to the current date";
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
