/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface Method
extends org.omg.java.cwm.objectmodel.behavioral.BehavioralFeature {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.ProcedureExpression getBody();

  public void setBody( org.omg.java.cwm.objectmodel.core.ProcedureExpression value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.behavioral.Operation getSpecification();

  public void setSpecification( org.omg.java.cwm.objectmodel.behavioral.Operation value );

}
