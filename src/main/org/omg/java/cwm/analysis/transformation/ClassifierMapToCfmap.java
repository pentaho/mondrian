/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface ClassifierMapToCfmap
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.analysis.transformation.ClassifierMap getClassifierMap( org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getCfMap( org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

}
