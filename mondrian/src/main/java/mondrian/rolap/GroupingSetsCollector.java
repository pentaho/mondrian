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
