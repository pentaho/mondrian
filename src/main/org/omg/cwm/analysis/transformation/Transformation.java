package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface Transformation extends Namespace {


// class scalar attributes
public ProcedureExpression getFunction();
public void setFunction( ProcedureExpression input);
public String getFunctionDescription();
public void setFunctionDescription( String input);
public Boolean getIsPrimary();
public void setIsPrimary( Boolean input);


// class references
public void setSource( Collection input);
public Collection getSource();
public void addSource( DataObjectSet input);
public void removeSource( DataObjectSet input);
public void setTarget( Collection input);
public Collection getTarget();
public void addTarget( DataObjectSet input);
public void removeTarget( DataObjectSet input);
public void setUse( Collection input);
public Collection getUse();
public void addUse( Dependency input);
public void removeUse( Dependency input);
// class operations
}

