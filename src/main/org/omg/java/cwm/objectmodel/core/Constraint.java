/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface Constraint
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.BooleanExpression getBody();

  public void setBody( org.omg.java.cwm.objectmodel.core.BooleanExpression value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getConstrainedElement();

}
