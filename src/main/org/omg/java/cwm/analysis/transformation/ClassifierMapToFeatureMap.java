/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface ClassifierMapToFeatureMap
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.analysis.transformation.FeatureMap featureMap, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.analysis.transformation.ClassifierMap getClassifierMap( org.omg.java.cwm.analysis.transformation.FeatureMap featureMap )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getFeatureMap( org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.analysis.transformation.FeatureMap featureMap, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.analysis.transformation.FeatureMap featureMap, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

}
