/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface DependencyClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.core.Dependency createDependency( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _kind )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Dependency createDependency();

}
