/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface InverseTransformationTask
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.analysis.transformation.TransformationTask originalTask, org.omg.java.cwm.analysis.transformation.TransformationTask inverseTask )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getInverseTask( org.omg.java.cwm.analysis.transformation.TransformationTask originalTask )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getOriginalTask( org.omg.java.cwm.analysis.transformation.TransformationTask inverseTask )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.analysis.transformation.TransformationTask originalTask, org.omg.java.cwm.analysis.transformation.TransformationTask inverseTask )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.analysis.transformation.TransformationTask originalTask, org.omg.java.cwm.analysis.transformation.TransformationTask inverseTask )
    throws javax.jmi.reflect.JmiException;

}
