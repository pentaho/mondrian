/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.expressions;



public interface OperationArgument
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.expressions.FeatureNode featureNode, org.omg.java.cwm.foundation.expressions.ExpressionNode argument )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getArgument( org.omg.java.cwm.foundation.expressions.FeatureNode featureNode )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.expressions.FeatureNode getFeatureNode( org.omg.java.cwm.foundation.expressions.ExpressionNode argument )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.expressions.FeatureNode featureNode, org.omg.java.cwm.foundation.expressions.ExpressionNode argument )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.expressions.FeatureNode featureNode, org.omg.java.cwm.foundation.expressions.ExpressionNode argument )
    throws javax.jmi.reflect.JmiException;

}
