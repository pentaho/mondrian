/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface ExpressionClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.core.Expression createExpression( java.lang.String _body, java.lang.String _language )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Expression createExpression();

}
