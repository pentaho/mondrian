/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationTarget
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.analysis.transformation.Transformation targetTransformation, org.omg.java.cwm.analysis.transformation.DataObjectSet target )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getTarget( org.omg.java.cwm.analysis.transformation.Transformation targetTransformation )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getTargetTransformation( org.omg.java.cwm.analysis.transformation.DataObjectSet target )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.analysis.transformation.Transformation targetTransformation, org.omg.java.cwm.analysis.transformation.DataObjectSet target )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.analysis.transformation.Transformation targetTransformation, org.omg.java.cwm.analysis.transformation.DataObjectSet target )
    throws javax.jmi.reflect.JmiException;

}
