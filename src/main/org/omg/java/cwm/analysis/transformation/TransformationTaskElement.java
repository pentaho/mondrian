/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationTaskElement
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.analysis.transformation.TransformationTask task, org.omg.java.cwm.analysis.transformation.Transformation transformation )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getTransformation( org.omg.java.cwm.analysis.transformation.TransformationTask task )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getTask( org.omg.java.cwm.analysis.transformation.Transformation transformation )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.analysis.transformation.TransformationTask task, org.omg.java.cwm.analysis.transformation.Transformation transformation )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.analysis.transformation.TransformationTask task, org.omg.java.cwm.analysis.transformation.Transformation transformation )
    throws javax.jmi.reflect.JmiException;

}
