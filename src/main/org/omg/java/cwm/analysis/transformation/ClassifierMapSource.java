/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface ClassifierMapSource
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Classifier source, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getClassifierMap( org.omg.java.cwm.objectmodel.core.Classifier source )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSource( org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Classifier source, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Classifier source, org.omg.java.cwm.analysis.transformation.ClassifierMap classifierMap )
    throws javax.jmi.reflect.JmiException;

}
