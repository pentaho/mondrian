package org. omg. cwm. objectmodel. behavioral;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Argument extends ModelElement
{
// class scalar attributes
public Expression getValue();


public void setValue( Expression input);
// class references
public void setCallAction( CallAction input);
public CallAction getCallAction();
// class operations
}

