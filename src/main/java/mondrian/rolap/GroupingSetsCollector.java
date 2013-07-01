/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
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
        assert groupingSets.isEmpty()
            || groupingSets.get(0).getColumns().length
            >= aggInfo.getColumns().length;
        groupingSets.add(aggInfo);
    }

    public List<GroupingSet> getGroupingSets() {
        return groupingSets;
    }
}

// End GroupingSetsCollector.java
