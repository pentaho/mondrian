/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface TaggedValueClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.core.TaggedValue createTaggedValue( java.lang.String _tag, java.lang.String _value )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.TaggedValue createTaggedValue();

}
