/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationTreeClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.analysis.transformation.TransformationTree createTransformationTree( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, org.omg.java.cwm.objectmodel.core.ProcedureExpression _function, java.lang.String _functionDescription, boolean _isPrimary, org.omg.java.cwm.analysis.transformation.TreeType _type, org.omg.java.cwm.foundation.expressions.ExpressionNode _body )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.analysis.transformation.TransformationTree createTransformationTree();

}
