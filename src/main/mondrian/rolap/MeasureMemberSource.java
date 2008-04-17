/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;

import java.util.List;

/**
 * A <code>MeasureMemberSource</code> implements the {@link MemberReader}
 * interface for the special Measures dimension.
 *
 * <p>Usually when a member is added to the context, the resulting SQL
 * statement has extra filters in its WHERE clause, but for members from this
 * source, but this implementation columns are added to the SELECT list.
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 */
class MeasureMemberSource extends ArrayMemberSource {
    MeasureMemberSource(
        RolapHierarchy hierarchy,
        List<RolapMember> memberList)
    {
        super(
            hierarchy,
            memberList.toArray(new RolapMember[memberList.size()]));
    }
}

// End MeasureMemberSource.java
