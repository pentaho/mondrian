/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface ClassifierFeature
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Classifier owner, org.omg.java.cwm.objectmodel.core.Feature feature )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getFeature( org.omg.java.cwm.objectmodel.core.Classifier owner )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Classifier getOwner( org.omg.java.cwm.objectmodel.core.Feature feature )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Classifier owner, org.omg.java.cwm.objectmodel.core.Feature feature )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Classifier owner, org.omg.java.cwm.objectmodel.core.Feature feature )
    throws javax.jmi.reflect.JmiException;

}
