/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface FeatureMapSource
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Feature source, org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getFeatureMap( org.omg.java.cwm.objectmodel.core.Feature source )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSource( org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Feature source, org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Feature source, org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

}
