/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface CfmapFeature
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Feature feature, org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getCfMap( org.omg.java.cwm.objectmodel.core.Feature feature )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getFeature( org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Feature feature, org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Feature feature, org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

}
