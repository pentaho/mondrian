/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.expressions;



public interface ExpressionNode
extends org.omg.java.cwm.objectmodel.core.Element {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Expression getExpression();

  public void setExpression( org.omg.java.cwm.objectmodel.core.Expression value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.foundation.expressions.FeatureNode getFeatureNode();

  public void setFeatureNode( org.omg.java.cwm.foundation.expressions.FeatureNode value );

  public org.omg.java.cwm.objectmodel.core.Classifier getType();

  public void setType( org.omg.java.cwm.objectmodel.core.Classifier value );

}
