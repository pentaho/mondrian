/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.instance;



public interface InstanceClassifier
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.instance.Instance instance, org.omg.java.cwm.objectmodel.core.Classifier classifier )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Classifier getClassifier( org.omg.java.cwm.objectmodel.instance.Instance instance )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getInstance( org.omg.java.cwm.objectmodel.core.Classifier classifier )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.instance.Instance instance, org.omg.java.cwm.objectmodel.core.Classifier classifier )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.instance.Instance instance, org.omg.java.cwm.objectmodel.core.Classifier classifier )
    throws javax.jmi.reflect.JmiException;

}
