package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface FeatureMap extends ModelElement {


// class scalar attributes
public ProcedureExpression getFunction();
public void setFunction( ProcedureExpression input);
public String getFunctionDescription();
public void setFunctionDescription( String input);
// class references
public void setSource( Collection input);
public Collection getSource();
public void addSource( Feature input);
public void removeSource( Feature input);
public void setTarget( Collection input);
public Collection getTarget();
public void addTarget( Feature input);
public void removeTarget( Feature input);
public void setClassifierMap( ClassifierMap input);
public ClassifierMap getClassifierMap();
// class operations
}

