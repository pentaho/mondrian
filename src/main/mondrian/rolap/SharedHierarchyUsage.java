/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.olap.Util;

import java.util.HashSet;
import java.util.HashMap;

/**
 * A usage of a shared hierarchy in the context of a particular cube.
 *
 * <p>A shared hierarchy can be used in several cubes.
 * It can also be used more than once in the same cube with different foreign
 * keys. (For exmaple, if the fact table has two foreign keys to the time
 * table, the cube would have two usages of the time dimension in different
 * roles.)
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
class SharedHierarchyUsage extends HierarchyUsage
{
    private final String sharedHierarchy;
    private RolapCube cube;

    SharedHierarchyUsage(
        RolapCube cube,
        String sharedHierarchy,
        String foreignKey)
    {
        super(cube.fact, foreignKey);
        this.cube = cube;
        this.sharedHierarchy = sharedHierarchy;
    }

    public boolean equals(Object o)
    {
        if (!(o instanceof SharedHierarchyUsage)) {
            return false;
        }
        SharedHierarchyUsage that = (SharedHierarchyUsage) o;
        return this.fact.equals(that.fact) &&
            this.sharedHierarchy.equals(that.sharedHierarchy) &&
            Util.equals(this.foreignKey, that.foreignKey);
    }

    public int hashCode()
    {
        int h = fact.hashCode();
        h = Util.hash(h, sharedHierarchy);
        h = Util.hash(h, foreignKey);
        return h;
    }
}

// End SharedHierarchyUsage.java
