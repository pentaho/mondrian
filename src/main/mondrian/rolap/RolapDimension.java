/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

/**
 * <code>RolapDimension</code> implements {@link Dimension} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapDimension extends DimensionBase
{
//  	/** @deprecated **/
//  	RolapDimension(String name, RolapHierarchy[] hierarchies)
//  	{
//  		this.name = name;
//  		this.uniqueName = Util.makeFqName(name);
//  		this.description = null;
//  		this.hierarchies = hierarchies;
//  	}

	RolapDimension(RolapCube cube, int ordinal, String name)
	{
		this.cube = cube;
		this.ordinal = ordinal;
		this.name = name;
		this.uniqueName = Util.makeFqName(name);
		this.description = null;
		this.dimensionType = name.equals("Time") ? TIME : STANDARD;
		this.hierarchies = new RolapHierarchy[0];
	}

	RolapDimension(
		RolapCube cube, int ordinal,
		MondrianDef.Dimension xmlDimension,
		MondrianDef.CubeDimension xmlCubeDimension)
	{
		this(cube, ordinal, xmlDimension.name);
		this.hierarchies = new RolapHierarchy[xmlDimension.hierarchies.length];
		for (int i = 0; i < xmlDimension.hierarchies.length; i++) {
			hierarchies[i] = new RolapHierarchy(
				this, xmlDimension.hierarchies[i], xmlCubeDimension);
		}
	}

	void init()
	{
		for (int i = 0; i < hierarchies.length; i++) {
			if (hierarchies[i] != null) {
				((RolapHierarchy) hierarchies[i]).init();
			}
		}
	}

	RolapHierarchy newHierarchy(
		String subName, boolean hasAll, String sql, String primaryKey,
		String foreignKey)
	{
		RolapHierarchy hierarchy = new RolapHierarchy(
			this, subName, hasAll, sql, primaryKey, foreignKey);
		this.hierarchies = (RolapHierarchy[]) RolapUtil.addElement(
			this.hierarchies, hierarchy);
		return hierarchy;
	}

	// implement Exp
	public Hierarchy getHierarchy() {
		return hierarchies[0];
	}
};



// End RolapDimension.java
