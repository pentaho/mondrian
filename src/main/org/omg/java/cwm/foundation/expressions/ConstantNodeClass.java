/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.expressions;



public interface ConstantNodeClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.expressions.ConstantNode createConstantNode( org.omg.java.cwm.objectmodel.core.Expression _expression, java.lang.Object _value )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.expressions.ConstantNode createConstantNode();

}
