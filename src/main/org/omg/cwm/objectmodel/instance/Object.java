package org. omg. cwm. objectmodel. instance;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Object extends Instance {


// class scalar attributes
// class references
public void setSlot( Collection input);


public Collection getSlot();
public void removeSlot( Slot input);
// class operations
}


