/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationMapClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.analysis.transformation.TransformationMap createTransformationMap( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, org.omg.java.cwm.objectmodel.core.ProcedureExpression _function, java.lang.String _functionDescription, boolean _isPrimary )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.analysis.transformation.TransformationMap createTransformationMap();

}
