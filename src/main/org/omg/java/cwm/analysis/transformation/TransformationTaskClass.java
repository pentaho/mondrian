/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationTaskClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.analysis.transformation.TransformationTask createTransformationTask( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isAbstract )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.analysis.transformation.TransformationTask createTransformationTask();

}
