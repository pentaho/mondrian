/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface Transformation
extends org.omg.java.cwm.objectmodel.core.Namespace {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.ProcedureExpression getFunction();

  public void setFunction( org.omg.java.cwm.objectmodel.core.ProcedureExpression value );

  public java.lang.String getFunctionDescription();

  public void setFunctionDescription( java.lang.String value );

  public boolean isPrimary();

  public void setPrimary( boolean value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getSource();

  public java.util.Collection getTarget();

  public java.util.Collection getUse();

}
