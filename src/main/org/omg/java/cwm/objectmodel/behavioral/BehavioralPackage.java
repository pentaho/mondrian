/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface BehavioralPackage
extends javax.jmi.reflect.RefPackage {

  public org.omg.java.cwm.objectmodel.core.CorePackage getCore();

  public org.omg.java.cwm.objectmodel.behavioral.ArgumentClass getArgument();

  public org.omg.java.cwm.objectmodel.behavioral.BehavioralFeatureClass getBehavioralFeature();

  public org.omg.java.cwm.objectmodel.behavioral.CallActionClass getCallAction();

  public org.omg.java.cwm.objectmodel.behavioral.EventClass getEvent();

  public org.omg.java.cwm.objectmodel.behavioral.InterfaceClass getInterface();

  public org.omg.java.cwm.objectmodel.behavioral.MethodClass getMethod();

  public org.omg.java.cwm.objectmodel.behavioral.OperationClass getOperation();

  public org.omg.java.cwm.objectmodel.behavioral.ParameterClass getParameter();

  public org.omg.java.cwm.objectmodel.behavioral.ParameterType getParameterType();

  public org.omg.java.cwm.objectmodel.behavioral.OperationMethod getOperationMethod();

  public org.omg.java.cwm.objectmodel.behavioral.CalledOperation getCalledOperation();

  public org.omg.java.cwm.objectmodel.behavioral.EventParameter getEventParameter();

  public org.omg.java.cwm.objectmodel.behavioral.CallArguments getCallArguments();

  public org.omg.java.cwm.objectmodel.behavioral.BehavioralFeatureParameter getBehavioralFeatureParameter();

}
