package org. omg. cwm. objectmodel. behavioral;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Method extends BehavioralFeature {


// class scalar attributes
public ProcedureExpression getBody();
public void setBody( ProcedureExpression input);
// class references
public void setSpecification( Operation input);
public Operation getSpecification();
// class operations
}


