/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface DependencyClient
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement client, org.omg.java.cwm.objectmodel.core.Dependency clientDependency )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getClientDependency( org.omg.java.cwm.objectmodel.core.ModelElement client )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getClient( org.omg.java.cwm.objectmodel.core.Dependency clientDependency )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement client, org.omg.java.cwm.objectmodel.core.Dependency clientDependency )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement client, org.omg.java.cwm.objectmodel.core.Dependency clientDependency )
    throws javax.jmi.reflect.JmiException;

}
