/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.expressions;



public interface NodeFeature
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Feature feature, org.omg.java.cwm.foundation.expressions.FeatureNode featureNode )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getFeatureNode( org.omg.java.cwm.objectmodel.core.Feature feature )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Feature getFeature( org.omg.java.cwm.foundation.expressions.FeatureNode featureNode )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Feature feature, org.omg.java.cwm.foundation.expressions.FeatureNode featureNode )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Feature feature, org.omg.java.cwm.foundation.expressions.FeatureNode featureNode )
    throws javax.jmi.reflect.JmiException;

}
