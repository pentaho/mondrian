/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface DocumentClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.businessinformation.Document createDocument( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _reference, java.lang.String _type )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.businessinformation.Document createDocument();

}
