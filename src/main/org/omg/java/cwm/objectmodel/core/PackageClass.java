/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface PackageClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.core.Package createPackage( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Package createPackage();

}
