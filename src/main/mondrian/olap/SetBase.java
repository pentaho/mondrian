/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

/**
 * todo:
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
class SetBase extends OlapElementBase implements Set {
	String name;
	Exp exp;

	SetBase(String name, Exp exp) {
		this.name = name;
		this.exp = exp;
	}

	// from Element
	public Object getObject() { return null; }
	public OlapElement getParent() { return null; }
	public String getUniqueName() { return "[" + name + "]"; }
	public String getName() { return name; }
	public String getQualifiedName() { return null; }
	public String getDescription() { return null; }

	// from Exp
	public int getType() { return CatSet; }
	public Hierarchy getHierarchy() { return exp.getHierarchy(); }
	public Cube getCube() { return exp.getCube(); }
	public OlapElement lookupChild(NameResolver st, String s)
		{ return null; }

	public void setName(String newName) {this.name = name;}

	public void accept(Visitor visitor)
	{
		visitor.visit(this);
	}
	public void childrenAccept(Visitor visitor)
	{
	}
}


// End SetBase.java
