/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface FeatureMap
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.ProcedureExpression getFunction();

  public void setFunction( org.omg.java.cwm.objectmodel.core.ProcedureExpression value );

  public java.lang.String getFunctionDescription();

  public void setFunctionDescription( java.lang.String value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getSource();

  public java.util.Collection getTarget();

  public org.omg.java.cwm.analysis.transformation.ClassifierMap getClassifierMap();

  public void setClassifierMap( org.omg.java.cwm.analysis.transformation.ClassifierMap value );

}
