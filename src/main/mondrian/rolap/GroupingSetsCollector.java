/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.rolap.agg.GroupingSet;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>The <code>GroupingSetsCollector</code> collects the GroupinpSets and pass
 * the consolidated list to form group by grouping sets sql</p>
 *
 * @author Thiyagu
 * @version $Id$
 * @since 06-Jun-2007
 */
public class GroupingSetsCollector {

    private final boolean useGroupingSets;

    private ArrayList<GroupingSet> groupingSets = new ArrayList<GroupingSet>();

    public GroupingSetsCollector(boolean useGroupingSets) {
        this.useGroupingSets = useGroupingSets;
    }

    public boolean useGroupingSets() {
        return useGroupingSets;
    }

    public void add(GroupingSet aggInfo) {
        assert groupingSets.isEmpty() ||
            groupingSets.get(0).getColumns().length >= aggInfo.getColumns().length;
        groupingSets.add(aggInfo);
    }

    public List<GroupingSet> getGroupingSets() {
        return groupingSets;
    }
}