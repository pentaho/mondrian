/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.expressions;



public interface ExpressionsPackage
extends javax.jmi.reflect.RefPackage {

  public org.omg.java.cwm.objectmodel.core.CorePackage getCore();

  public org.omg.java.cwm.foundation.expressions.ExpressionNodeClass getExpressionNode();

  public org.omg.java.cwm.foundation.expressions.ConstantNodeClass getConstantNode();

  public org.omg.java.cwm.foundation.expressions.ElementNodeClass getElementNode();

  public org.omg.java.cwm.foundation.expressions.FeatureNodeClass getFeatureNode();

  public org.omg.java.cwm.foundation.expressions.ReferencedElement getReferencedElement();

  public org.omg.java.cwm.foundation.expressions.OperationArgument getOperationArgument();

  public org.omg.java.cwm.foundation.expressions.NodeFeature getNodeFeature();

  public org.omg.java.cwm.foundation.expressions.ExpressionNodeClassifier getExpressionNodeClassifier();

}
