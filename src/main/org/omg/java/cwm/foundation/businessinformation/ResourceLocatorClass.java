/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface ResourceLocatorClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.businessinformation.ResourceLocator createResourceLocator( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _url )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.businessinformation.ResourceLocator createResourceLocator();

}
