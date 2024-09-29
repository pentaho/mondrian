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

import mondrian.olap.MondrianDef;

/**
 * Provides the base cubes that a virtual cube uses and
 * specifies if unrelated dimensions to measures from these cubes should be
 * ignored.
 *
 * @author ajoglekar
 * @since Nov 22 2007
 */
public class RolapCubeUsages {
    private MondrianDef.CubeUsages cubeUsages;

    public RolapCubeUsages(MondrianDef.CubeUsages cubeUsage) {
        this.cubeUsages = cubeUsage;
    }

    public boolean shouldIgnoreUnrelatedDimensions(String baseCubeName) {
        if (cubeUsages == null || cubeUsages.cubeUsages == null) {
            return false;
        }
        for (MondrianDef.CubeUsage usage : cubeUsages.cubeUsages) {
            if (usage.cubeName.equals(baseCubeName)
                && Boolean.TRUE.equals(usage.ignoreUnrelatedDimensions))
            {
                return true;
            }
        }
        return false;
    }
}

// End RolapCubeUsages.java
