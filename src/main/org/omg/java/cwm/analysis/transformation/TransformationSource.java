/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationSource
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.analysis.transformation.Transformation sourceTransformation, org.omg.java.cwm.analysis.transformation.DataObjectSet source )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSource( org.omg.java.cwm.analysis.transformation.Transformation sourceTransformation )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSourceTransformation( org.omg.java.cwm.analysis.transformation.DataObjectSet source )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.analysis.transformation.Transformation sourceTransformation, org.omg.java.cwm.analysis.transformation.DataObjectSet source )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.analysis.transformation.Transformation sourceTransformation, org.omg.java.cwm.analysis.transformation.DataObjectSet source )
    throws javax.jmi.reflect.JmiException;

}
