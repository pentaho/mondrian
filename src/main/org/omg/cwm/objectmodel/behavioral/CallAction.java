package org. omg. cwm. objectmodel. behavioral;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface CallAction extends ModelElement {


// class scalar attributes
// class references
public void setOperation( Operation input);
public Operation getOperation();
public void setActualArgument( Collection input);
public List getActualArgument();
public void removeActualArgument( Argument input);
public void moveActualArgumentBefore( Argument before, Argument input);


public void moveActualArgumentAfter( Argument before, Argument input);
// class operations
}


