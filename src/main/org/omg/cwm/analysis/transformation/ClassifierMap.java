package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface ClassifierMap extends Namespace {


// class scalar attributes
public ProcedureExpression getFunction();
public void setFunction( ProcedureExpression input);
public String getFunctionDescription();
public void setFunctionDescription( String input);


// class references
public void setSource( Collection input);
public Collection getSource();
public void addSource( Classifier input);
public void removeSource( Classifier input);
public void setTarget( Collection input);
public Collection getTarget();
public void addTarget( Classifier input);
public void removeTarget( Classifier input);
public void setTransformationMap( Namespace input);
public Namespace getTransformationMap();
public void setFeatureMap( Collection input);
public Collection getFeatureMap();
public void removeFeatureMap( FeatureMap input);
public void setCfMap( Collection input);
public Collection getCfMap();
public void removeCfMap( ClassifierFeatureMap input);
// class operations
}


