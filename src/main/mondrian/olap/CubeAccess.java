/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1999-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// lkrivopaltsev, 01 November, 1999
*/

package mondrian.olap;
import mondrian.resource.MondrianResource;

import java.util.List;
import java.util.ArrayList;

/**
 * This class implements object of type GrantCube to apply permissions
 * on user's MDX query
 */
public class CubeAccess {

    private boolean hasRestrictions;
    /** array of hierarchies with no access */
    private Hierarchy[] noAccessHierarchies;
    /** array of members which have limited access */
    private Member[]  limitedMembers;
    private final List hierarchyList;
    private final List memberList;
    private final Cube mdxCube;

    /**
     * Creates a CubeAccess object.
     *
     * <p>User's code should be responsible for
     * filling cubeAccess with restricted hierarchies and restricted
     * members by calling addSlicer(). Do NOT forget to call
     * {@link #normalizeCubeAccess()} after you done filling cubeAccess.
     */
    public CubeAccess(Cube mdxCube) {
        this.mdxCube = mdxCube;
        noAccessHierarchies = null;
        limitedMembers = null;
        hasRestrictions = false;
        hierarchyList = new ArrayList();
        memberList = new ArrayList();
    }

    public boolean hasRestrictions() {
        return hasRestrictions;
    }
    public Hierarchy[] getNoAccessHierarchies() {
        return noAccessHierarchies;
    }
    public Member[] getLimitedMembers() {
        return limitedMembers;
    }
    public List getNoAccessHierarchyList() {
        return hierarchyList;
    }
    public List getLimitedMemberList() {
        return memberList;
    }
    public boolean isHierarchyAllowed(Hierarchy mdxHierarchy) {
        String hierName = mdxHierarchy.getUniqueName();
        if(noAccessHierarchies == null || hierName == null) {
            return true;
        }
        for(int i = 0; i < noAccessHierarchies.length; i++) {
            if(hierName.equalsIgnoreCase(noAccessHierarchies[i].getUniqueName()) ) {
                return false;
            }
        }
        return true;
    }

    public Member getLimitedMemberForHierarchy(Hierarchy mdxHierarchy) {
        String hierName = mdxHierarchy.getUniqueName();
        if (limitedMembers == null || hierName == null) {
            return null;
        }
        for (int i = 0; i < limitedMembers.length; i++) {
            Hierarchy limitedHierarchy =
                limitedMembers[i].getHierarchy();
            if (hierName.equalsIgnoreCase(limitedHierarchy.getUniqueName())) {
                return limitedMembers[i];
            }
        }
        return null;
    }

    /**
     * Adds  restricted hierarchy or limited member based on bMember
     */
    public void addGrantCubeSlicer(String sHierarchy,
                                   String sMember,
                                   boolean bMember) {
        if (bMember) {
            boolean fail = false;
            String[] sMembers = Util.explode(sMember);
            SchemaReader schemaReader = mdxCube.getSchemaReader(null);
            Member member = schemaReader.getMemberByUniqueName(sMembers, fail);
            if (member == null) {
                throw MondrianResource.instance().MdxCubeSlicerMemberError.ex(
                    sMember, sHierarchy, mdxCube.getUniqueName());
            }
            // there should be only slicer per hierarchy; ignore the rest
            if (getLimitedMemberForHierarchy(member.getHierarchy()) == null) {
                memberList.add(member);
            }
        } else {
            boolean fail = false;
            Hierarchy hierarchy = mdxCube.lookupHierarchy(sHierarchy, fail);
            if (hierarchy == null) {
                throw MondrianResource.instance().MdxCubeSlicerHierarchyError.ex(
                    sHierarchy, mdxCube.getUniqueName());
            }
            hierarchyList.add(hierarchy);
        }
    }

    /** Initializes internal arrays of restricted hierarchies and limited
     * members. It has to be called  after all 'addSlicer()' calls.
     */
    public void normalizeCubeAccess() {
        if (memberList.size() > 0) {
            limitedMembers = (Member[])
                memberList.toArray(new Member[memberList.size()]);
            hasRestrictions = true;
        }
        if (hierarchyList.size() > 0) {
            noAccessHierarchies = (Hierarchy[])
                hierarchyList.toArray(new Hierarchy[ hierarchyList.size()]);
            hasRestrictions = true;
        }
    }

    /**compares this CubeAccess to the specified Object
     */
    public boolean equals(Object object) {
        if (!(object instanceof CubeAccess)) {
           return false;
        }
        CubeAccess cubeAccess = (CubeAccess) object;
        List hierarchyList = cubeAccess.getNoAccessHierarchyList();
        List limitedMemberList = cubeAccess.getLimitedMemberList();

        if ((this.hierarchyList.size() != hierarchyList.size()) ||
            (this.memberList.size() != limitedMemberList.size())) {
            return false;
        }
        for (int i = 0; i < hierarchyList.size(); i++) {
            if (!this.hierarchyList.contains(hierarchyList.get(i))) {
                return false;
            }
        }
        for (int i = 0; i < limitedMemberList.size(); i++ ) {
            if (!this.memberList.contains( limitedMemberList.get(i))) {
                return false;
            }
        }
        return true;
    }
}

// End CubeAccess.java
