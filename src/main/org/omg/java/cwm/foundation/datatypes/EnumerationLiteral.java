/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface EnumerationLiteral
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Expression getValue();

  public void setValue( org.omg.java.cwm.objectmodel.core.Expression value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.foundation.datatypes.Enumeration getEnumeration();

  public void setEnumeration( org.omg.java.cwm.foundation.datatypes.Enumeration value );

}
