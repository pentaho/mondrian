/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface EmailClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.businessinformation.Email createEmail( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _emailAddress, java.lang.String _emailType )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.businessinformation.Email createEmail();

}
