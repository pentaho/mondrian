/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationStepTask
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.analysis.transformation.TransformationStep step, org.omg.java.cwm.analysis.transformation.TransformationTask task )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.analysis.transformation.TransformationTask getTask( org.omg.java.cwm.analysis.transformation.TransformationStep step )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getStep( org.omg.java.cwm.analysis.transformation.TransformationTask task )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.analysis.transformation.TransformationStep step, org.omg.java.cwm.analysis.transformation.TransformationTask task )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.analysis.transformation.TransformationStep step, org.omg.java.cwm.analysis.transformation.TransformationTask task )
    throws javax.jmi.reflect.JmiException;

}
