package org. omg. cwm. objectmodel. behavioral;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Event extends ModelElement {


// class scalar attributes
// class references
public void setParameter( Collection input);


public List getParameter();
public void removeParameter( Parameter input);
public void moveParameterBefore( Parameter before, Parameter input);
public void moveParameterAfter( Parameter before, Parameter input);
// class operations
}

