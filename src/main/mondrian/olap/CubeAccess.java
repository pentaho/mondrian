/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// lkrivopaltsev, 01 November, 1999
*/

package mondrian.olap;
import java.io.*;
import java.util.*;

/**
 * This class implements object of type GrantCube to apply permissions
 * on user's MDX query
 **/
public class CubeAccess
{
	private boolean bHasRestrictions; 
	Hierarchy[] noAccessHierarchies; // array of hierarchies with no access
	Member[]  limitedMembers;  // array of limitedMembers
	Vector vHierarchy;
	Vector vMember;
	Cube mdxCube;
	
	/**Creates cubeAccess object. User's code should be responsible for
	 * filling cubeAccess with restricted hierarchies and restricted 
	 * members by calling addSlicer(). Do NOT forfet to call
	 *  'normalizeCubeAccess()' after you done filling cubeAccess.
	 */
	public CubeAccess( Cube mdxCube )
	{
		this.mdxCube = mdxCube;
		noAccessHierarchies = null;
		limitedMembers = null;
		bHasRestrictions = false;
		vHierarchy = new Vector();
		vMember = new Vector();
	}
	
	public boolean hasRestrictions(){ return bHasRestrictions; }
	public Hierarchy[] getNoAccessHierarchies(){return noAccessHierarchies;}
	public Member[] getLimitedMembers(){return limitedMembers; }
	public Vector getNoAccessHierarchiesAsVector(){ return vHierarchy; }
	public Vector getLimitedMembersAsVector(){ return vMember; }
	public boolean isHierarchyAllowed( Hierarchy mdxHierarchy )
	{
		String hierName = mdxHierarchy.getUniqueName();
		if( noAccessHierarchies == null || hierName == null ){
			return true;
		}
		for( int i = 0; i < noAccessHierarchies.length; i++ ){
			if( hierName.equalsIgnoreCase(noAccessHierarchies[i].getUniqueName()) ){
				return false;
			}
		}
		return true;
	}
	
	public Member getLimitedMemberForHierarchy(Hierarchy mdxHierarchy)
	{
		String hierName = mdxHierarchy.getUniqueName();
		if (limitedMembers == null || hierName == null) {
			return null;
		}
		for (int i = 0; i < limitedMembers.length; i++) {
			Hierarchy limitedHierarchy =
				limitedMembers[i].getHierarchy();
			if (hierName.equalsIgnoreCase( limitedHierarchy.getUniqueName())) {
				return limitedMembers[i];
			}
		}
		return null;
	}
	
	/**
	 * Adds  restricted hierarchy or limited member based on bMember
	 */
	public void addGrantCubeSlicer(
		String sHierarchy, String sMember, boolean bMember)
	{
		if (bMember) {
			boolean fail = false;
			Member member = mdxCube.lookupMemberByUniqueName(sMember, fail);
			if (member == null) {
				throw Util.getRes().newMdxCubeSlicerMemberError(
					sMember, sHierarchy, mdxCube.getUniqueName());
			}
			// there should be only slicer per hierarchy; ignore the rest
			if (getLimitedMemberForHierarchy(member.getHierarchy()) == null) {
				vMember.addElement(member);
			}
		} else {
			boolean fail = false;
			Hierarchy hierarchy = mdxCube.lookupHierarchy(sHierarchy, fail);
			if (hierarchy == null) {
				throw Util.getRes().newMdxCubeSlicerHierarchyError(
					sHierarchy, mdxCube.getUniqueName());
			}
			vHierarchy.addElement(hierarchy);
		}
	}

	/** Initializes internal arrays of restricted hierarchies and limited
	 * members. It has to be called  after all 'addSlicer()' calls.
	 */
	public void normalizeCubeAccess()
	{
		if( vMember.size() > 0 ){
			limitedMembers = new Member[ vMember.size()];
			vMember.copyInto(limitedMembers);
			bHasRestrictions = true;
		}
		if( vHierarchy.size() > 0 ){
			noAccessHierarchies = new Hierarchy[ vHierarchy.size()];
			vHierarchy.copyInto(noAccessHierarchies);
			bHasRestrictions = true;
		}
	}
	
	/**compares this CubeAccess to the specified Object
	 */
	public boolean equals( Object object )
	{
		if( !( object instanceof CubeAccess )){
		   return false;
		}
		CubeAccess cubeAccess = ( CubeAccess ) object;
		Vector vMdxHierarchies = cubeAccess.getNoAccessHierarchiesAsVector();
		Vector vMembers = cubeAccess.getLimitedMembersAsVector();

		if(( this.vHierarchy.size() != vMdxHierarchies.size() ) ||
		   ( this.vMember.size() != vMembers.size() )){
			return false;
		}		
		for( int i = 0; i < vMdxHierarchies.size(); i++ ){
			if( !this.vHierarchy.contains( vMdxHierarchies.elementAt(i))){
				return false;
			}
		}
		for( int i = 0; i < vMembers.size(); i++ ){
			if( !this.vMember.contains( vMembers.elementAt(i ))){
				return false;
			}
		}
		return true;
	}
		
}

// End CubeAccess.java
