/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface QueryExpressionClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.datatypes.QueryExpression createQueryExpression( java.lang.String _body, java.lang.String _language )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.datatypes.QueryExpression createQueryExpression();

}
