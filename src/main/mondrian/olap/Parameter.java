/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// leonardk, 10 January, 2000
*/

package mondrian.olap;
import java.util.*;
import java.io.*;

public class Parameter extends ExpBase {
	String name;
	int category; // CatString, CatNumeric, or CatMember
	Hierarchy hierarchy;
	String hierarchyUniqueName; // if type == hierarchy
	String defaultValue;
	String currentValue; // if null, use default
	/** the Member underlying currentValue. Is null if parameter is not a
	 * hierarchy, or if the member does not exist (which is valid, since the
	 * cube may not have been loaded yet). */
	Member mdxMember;
	String description;
	int nDefines;
	/** how many times is object used in the query **/
	int  nUses;
	/** unparse() needs that flag to decide if it needs to print definition **/
	boolean bDefPrinted;
	/** this is flag to delete the unused parameter **/
	boolean toBeDeleted;

	Parameter( FunCall fParam )
	{
		nUses = 0;
		nDefines = 0;
		bDefPrinted = false;
		toBeDeleted = false;
		update( fParam );
	}

	Parameter(
		String name, int category, Hierarchy hierarchy, String defaultValue,
		String currentValue, boolean toBeDeleted, Member mdxMember,
		String description)
	{
		this.name = name;
		this.category = category;
		this.hierarchy = hierarchy;
		if (hierarchy != null) {
			this.hierarchyUniqueName = hierarchy.getUniqueName();
		}
		this.defaultValue = defaultValue;
		this.currentValue = currentValue;
		this.nUses = 0;
		this.bDefPrinted = false;
		this.nDefines = 1;
		this.toBeDeleted = toBeDeleted;
		this.mdxMember = mdxMember;
		this.description = description;
	}

	static Parameter[] cloneArray(Parameter[] a)
		throws CloneNotSupportedException
	{
		if (a == null)
			return null;
		Parameter[] a2 = new Parameter[a.length];
		for (int i = 0; i < a.length; i++)
			a2[i] = (Parameter) a[i].clone();
		return a2;
	}

	public int getType() {
		return category;
	}

	// this function updates certain properties of Parameter object
	void update( FunCall fParam )
	{
		for( int i = 0; i < fParam.args.length; i++ ){
			if( i != 1 ){
				if (!(fParam.args[i] instanceof Literal)) {
					throw Util.getRes().newMdxParamArgInvalid(
						fParam.args[i].toString(), fParam.args[0].toString());
				}
			} else {
				if (!(fParam.args[i] instanceof Literal ||
					  fParam.args[i] instanceof Dimension)) {
					throw Util.getRes().newMdxParamTypeInvalid(
						fParam.args[i].toString(), fParam.args[0].toString());
				}
			}
		}

		// If this is not the first time we've seen this parameter, check that
		// the name is the same.
		String name = (String) ((Literal)(fParam.args[0])).getValue();
		if (this.name != null) {
			Util.assertTrue(
				name.equals(this.name), "parameter renamed");
		} else {
			this.name = name;
		}

		if( fParam.getFunName().equalsIgnoreCase( "Parameter" )) {
			//parameter definition
			++nDefines;

			// Get the type
			if( fParam.args[1] instanceof Literal ){
				String val = (String) ((Literal)(fParam.args[1])).getValue();
				if (val.equalsIgnoreCase("string")) {
					category = CatString;
				} else if (val.equalsIgnoreCase("number")) {
					category = CatNumeric;
				} else {
					category = CatMember;
					hierarchyUniqueName = val;
				}
			} else {
				category = CatMember;
				hierarchyUniqueName = ((Dimension)(fParam.args[1])).getUniqueName();
			}
			defaultValue = (String) ((Literal)(fParam.args[2])).getValue();
			if( fParam.args.length > 3){
				description = (String) ((Literal)(fParam.args[3])).getValue();
			}
		} else if( fParam.getFunName().equalsIgnoreCase( "ParamRef") ){
			//parameter reference
			if( fParam.args.length > 1 ){
				currentValue = (String) ((Literal)(fParam.args[1])).getValue();
			}
		} else {
			throw Util.getRes().newInternal(
				"Parser error: Unknown symbol " + fParam.args[0].toString());
		}
	}

	public boolean usesDimension(Dimension dimension)
	{
		Hierarchy mdxHierarchy = getHierarchy();
		return mdxHierarchy != null &&
			mdxHierarchy.getDimension() == dimension;
	}

	public Exp resolve(Query query)
	{
		// There must be some Parameter with this name registered with the
		// Query.  After clone(), there will be many copies of the same
		// parameter, and we rely on this method to bring them down to one.
		// So if this object is not the registered vesion, that's fine, go with
		// the other one.  The registered one will be resolved after everything
		// else in the query has been resolved.
		Parameter p = query.lookupParam( name );
		Util.assertTrue(
			p != null, "parameter '" + name + "' not registered");
		if (p != this) {
			return p;			// will resolve it later
		}

		hierarchy = null;
		if (hierarchyUniqueName != null) {
			hierarchy = query.getCube().lookupHierarchy(
				hierarchyUniqueName, true);
			setMember(query);
		}

		return this;
	}

	public String getName()
	{return name;}

	public String getValue()
	{
		if( currentValue != null ){
			return currentValue;
		}
		return defaultValue;
	}

	public String getDefaultValue()
	{
		return defaultValue;
	}

	public void setValue( String value, NameResolver st )
	{
		currentValue = value;
		setMember( st );
	}

	public String getParameterType() {
		return category == CatString ? "string" :
			category == CatNumeric  ? "number" :
			"member";
	}

	public String getHierarchyName() {
		return hierarchyUniqueName;
	}

	public String getMemberCaption() {
		return mdxMember == null ? null : mdxMember.getCaption();
	}

	public String getDescription() {
		return description;
	}

	public void validate(Query q) {
		if (nDefines != 1) {
			throw q.getError().newMdxParamMultipleDef( name, nDefines );
		}
		if (!(defaultValue != null || currentValue != null)) {
			throw q.getError().newMdxParamValueNotFound(name);
		}
	}

	public Hierarchy getHierarchy() {
		return hierarchy;
	}

	public Member getMember()
	{
		return mdxMember;
	}

	public Member getMember(NameResolver st)
	{
		if (hierarchyUniqueName != null) {
			String sMember = getValue();
			return Util.lookupMember(st, sMember, true);
		}
		return null;
	}

	public Object clone()
	{
		return new Parameter(
			name, category, hierarchy, defaultValue,
			currentValue, toBeDeleted, mdxMember, description);
	}

	public void unparse(PrintWriter pw, ElementCallback callback)
	{
		String value = getValue();
		if (!callback.isPlatoMdx()) {
			// reconstructing query for the webUI
			String parameter = null;
			String commaSpace = ", ";
			if (!bDefPrinted) {
				if (hierarchyUniqueName != null &&
					!hierarchyUniqueName.equals("")) {
					Dimension mdxDim = getHierarchy().getDimension();
					Util.assertTrue(
						mdxDim != null,
						"No dimension associated with member parameter " +
						name);
					String hierStr = callback.registerItself(mdxDim);
					if (hierStr == null) {
						hierStr = hierarchyUniqueName;
					}
					String quotedValue = null;
					if (mdxMember != null) {
						quotedValue = callback.registerItself(mdxMember);
					}
					if (quotedValue != null) {
						// do not escape if it's from BAFL!!
						quotedValue = "\"" + quotedValue + "\"";
					} else {
						quotedValue = Util.quoteForMdx(value);
					}
					parameter = "Parameter(" + Util.quoteForMdx(name) +
						commaSpace + hierStr + commaSpace + quotedValue;
				} else {
					parameter = "Parameter(" +
						Util.quoteForMdx(name) + commaSpace +
						Util.quoteForMdx(getParameterType()) + commaSpace +
						Util.quoteForMdx(value);
				}
				if (description != null) {
					parameter += commaSpace + Util.quoteForMdx(description);
				}
				parameter += ")";
				bDefPrinted = true;
			} else {
				parameter = "ParamRef(" + Util.quoteForMdx(name) + ")";
			}
			pw.print(parameter);
		} else {
			// constructing query for plato
			if (category != CatString) {
				pw.print(value);
			} else {
				pw.print(Util.quoteForMdx(value));
			}
		}
	}

	// For the purposes of type inference and expression substitution, a
	// parameter is atomic; therefore, we ignore the child member, if any.
	public Object[] getChildren()
	{
		return null;
	}

	public boolean isUsed()
	{
		if( this.nUses > 0 ){
			return true;
		} else {
			this.toBeDeleted = true;
			return false;
		}
	}

	public boolean isToBeDeleted()
	{ return toBeDeleted;}

	void setMember(NameResolver st)
	{
		if (category == CatMember) {
			mdxMember = Util.lookupMember(st, getValue(), false);
		}
	}

	/** Returns whether <code>this</code> is equal to <code>other</code>
	 * Parameter based on name, type and value */
	public boolean equals( Object other )
	{
		if( !( other instanceof Parameter )){
			return false;
		}
		Parameter otherParameter = ( Parameter ) other;
		return otherParameter.getName().equals(this.getName()) &&
			otherParameter.getType() == this.getType() &&
			otherParameter.getValue().equals(this.getValue());
	}

	public void resetPrintProperty()
	{ this.bDefPrinted = false;}


	// implement Exp
	public Object evaluate(Evaluator evaluator)
	{
		return evaluator.xx(this);
	}
}


// End Parameter.java

