/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationStep
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.analysis.transformation.TransformationTask getTask();

  public void setTask( org.omg.java.cwm.analysis.transformation.TransformationTask value );

  public org.omg.java.cwm.objectmodel.core.Namespace getActivity();

  public void setActivity( org.omg.java.cwm.objectmodel.core.Namespace value );

  public java.util.Collection getPrecedence();

  public java.util.Collection getPrecedingStep();

  public java.util.Collection getSucceedingStep();

}
