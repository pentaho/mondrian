/*
// $Id$
//
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
//
// inna, 4 Oct, 1999
*/

package mondrian.olap;

/**
 * Callback to tell {@link Query#unparse} how to do its job.
 **/
public class ElementCallback
{

	/**
	 * This method is called for each object in the parse tree.  If it does not
	 * return null, use the return value as the object's print name.
	 */
	public String registerItself(OlapElement elm) {
		return null;
	}

	/**
	 * @return whether to expand parameters and lie about axis names (because
	 * the query is going to Plato)
	 */
	public boolean isPlatoMdx() {
		return false;
	}
	
	/**
	 * If hidden calculated member exists for given uName, it returns it.
	 *
	 * This method is used to format existing members.
	 */
	public String findHiddenName(String uName)
	{return null;}
	
	/** disables or enables hidden name lookup*/
	public void disableHiddenNameLookup(boolean disableLookup)
	{}
	
		
}

// End ElementCallback.java
