/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface BehavioralFeatureParameter
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.behavioral.BehavioralFeature behavioralFeature, org.omg.java.cwm.objectmodel.behavioral.Parameter parameter )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getParameter( org.omg.java.cwm.objectmodel.behavioral.BehavioralFeature behavioralFeature )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.behavioral.BehavioralFeature getBehavioralFeature( org.omg.java.cwm.objectmodel.behavioral.Parameter parameter )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.behavioral.BehavioralFeature behavioralFeature, org.omg.java.cwm.objectmodel.behavioral.Parameter parameter )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.behavioral.BehavioralFeature behavioralFeature, org.omg.java.cwm.objectmodel.behavioral.Parameter parameter )
    throws javax.jmi.reflect.JmiException;

}
