/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface DependencySupplier
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement supplier, org.omg.java.cwm.objectmodel.core.Dependency supplierDependency )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSupplierDependency( org.omg.java.cwm.objectmodel.core.ModelElement supplier )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSupplier( org.omg.java.cwm.objectmodel.core.Dependency supplierDependency )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement supplier, org.omg.java.cwm.objectmodel.core.Dependency supplierDependency )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement supplier, org.omg.java.cwm.objectmodel.core.Dependency supplierDependency )
    throws javax.jmi.reflect.JmiException;

}
