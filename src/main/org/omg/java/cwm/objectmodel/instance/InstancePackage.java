/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.instance;



public interface InstancePackage
extends javax.jmi.reflect.RefPackage {

  public org.omg.java.cwm.objectmodel.core.CorePackage getCore();

  public org.omg.java.cwm.objectmodel.instance.InstanceClass getInstance();

  public org.omg.java.cwm.objectmodel.instance.SlotClass getSlot();

  public org.omg.java.cwm.objectmodel.instance.DataValueClass getDataValue();

  public org.omg.java.cwm.objectmodel.instance.ObjectClass getObject();

  public org.omg.java.cwm.objectmodel.instance.ExtentClass getExtent();

  public org.omg.java.cwm.objectmodel.instance.FeatureSlot getFeatureSlot();

  public org.omg.java.cwm.objectmodel.instance.ObjectSlot getObjectSlot();

  public org.omg.java.cwm.objectmodel.instance.InstanceClassifier getInstanceClassifier();

  public org.omg.java.cwm.objectmodel.instance.SlotValue getSlotValue();

}
