/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.olap.fun;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;

/**
 * A <code>ParameterFunDef</code> is a pseudo-function describing calls to
 * <code>Parameter</code> and <code>ParamRef</code> functions. It exists only
 * fleetingly, and is then converted into a {@link mondrian.olap.Parameter}.
 * For internal use only. 
 *
 * @author jhyde
 * @since Feb 14, 2003
 * @version $Id$
 **/
public class ParameterFunDef extends FunDefBase {
	public String parameterName;
	private Hierarchy hierarchy;
	public Exp exp;
	public String parameterDescription;

	ParameterFunDef(FunDef funDef, String parameterName, Hierarchy hierarchy,
					int returnType, Exp exp, String description) {
		super(funDef);
		assertPrecondition(getName().equals("Parameter") ||
				getName().equals("ParamRef"));
		this.parameterName = parameterName;
		this.hierarchy = hierarchy;
		this.returnType = returnType;
		this.exp = exp;
		this.parameterDescription = description;
	}

	/**
	 * Whether this function call defines a parameter ("Parameter"), as opposed
	 * to referencing one ("ParamRef").
	 */
	public boolean isDefinition() {
		return getName().equals("Parameter");
	}

	public Hierarchy getHierarchy(Exp[] args) {
		return hierarchy;
	}
}

// End ParameterFunDef.java
