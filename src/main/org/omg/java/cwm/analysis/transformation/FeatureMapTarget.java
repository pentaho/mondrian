/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface FeatureMapTarget
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Feature target, org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getFeatureMap( org.omg.java.cwm.objectmodel.core.Feature target )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getTarget( org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Feature target, org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Feature target, org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

}
