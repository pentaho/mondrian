package javax.jmi.reflect;

import java.util.Collection;

public interface RefAssociation extends RefBaseObject {
    public Collection refAllLinks();
    public boolean refLinkExists(RefObject firstEnd, RefObject secondEnd);
    public Collection refQuery(RefObject queryEnd, RefObject queryObject);
    public Collection refQuery(String endName, RefObject queryObject);
    public boolean refAddLink(RefObject firstEnd, RefObject secondEnd);
    public boolean refRemoveLink(RefObject firstEnd, RefObject secondEnd);
}
