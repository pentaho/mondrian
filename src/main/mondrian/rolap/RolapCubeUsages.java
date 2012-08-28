/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;

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
