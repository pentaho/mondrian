/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationTree
extends org.omg.java.cwm.analysis.transformation.Transformation {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.analysis.transformation.TreeType getType();

  public void setType( org.omg.java.cwm.analysis.transformation.TreeType value );

  public org.omg.java.cwm.foundation.expressions.ExpressionNode getBody();

  public void setBody( org.omg.java.cwm.foundation.expressions.ExpressionNode value );

}
