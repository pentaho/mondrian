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

import mondrian.olap.Util;

/**
 * The 'All' member of a {@link mondrian.rolap.RolapCubeHierarchy}.
 *
 * <p>A minor extension to {@link mondrian.rolap.RolapCubeMember} because the
 * naming rules are different.
 *
 * @author Will Gorman, 19 October 2007
 */
class RolapAllCubeMember
    extends RolapCubeMember
{
    protected final String name;
    private final String uniqueName;

    /**
     * Creates a RolapAllCubeMember.
     *
     * @param member Member of underlying (non-cube) hierarchy
     * @param cubeLevel Level
     */
    public RolapAllCubeMember(RolapMember member, RolapCubeLevel cubeLevel)
    {
        super(null, member, cubeLevel);
        assert member.isAll();

        // replace hierarchy name portion of all member with new name
        if (member.getHierarchy().getName().equals(getHierarchy().getName())) {
            name = member.getName();
        } else {
            // special case if we're dealing with a closure
            String replacement =
                getHierarchy().getName().replaceAll("\\$", "\\\\\\$");

            // convert string to regular expression
            String memberLevelName =
                member.getHierarchy().getName().replaceAll("\\.", "\\\\.");

            name = member.getName().replaceAll(memberLevelName, replacement);
        }

        // Assign unique name. We use a kludge to ensure that calc members are
        // called [Measures].[Foo] not [Measures].[Measures].[Foo]. We can
        // remove this code when we revisit the scheme to generate member unique
        // names.
        if (getHierarchy().getName().equals(getDimension().getName())) {
            this.uniqueName = Util.makeFqName(getDimension(), name);
        } else {
            this.uniqueName = Util.makeFqName(getHierarchy(), name);
        }
    }

    public String getName() {
        return name;
    }

    @Override
    public String getUniqueName() {
        return uniqueName;
    }
}

// End RolapAllCubeMember.java
