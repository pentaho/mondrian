/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.expressions;



public interface ElementNodeClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.expressions.ElementNode createElementNode( org.omg.java.cwm.objectmodel.core.Expression _expression )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.expressions.ElementNode createElementNode();

}
