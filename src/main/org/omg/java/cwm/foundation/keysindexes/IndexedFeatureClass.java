/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface IndexedFeatureClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.keysindexes.IndexedFeature createIndexedFeature( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.Boolean _isAscending )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.keysindexes.IndexedFeature createIndexedFeature();

}
