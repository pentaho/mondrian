/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface CfmapClassifier
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Classifier classifier, org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getCfMap( org.omg.java.cwm.objectmodel.core.Classifier classifier )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getClassifier( org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Classifier classifier, org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Classifier classifier, org.omg.java.cwm.analysis.transformation.ClassifierFeatureMap cfMap )
    throws javax.jmi.reflect.JmiException;

}
