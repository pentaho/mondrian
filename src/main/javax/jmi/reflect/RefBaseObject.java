package javax.jmi.reflect;

import java.util.Collection;

public interface RefBaseObject {
    public RefObject refMetaObject();
    public RefPackage refImmediatePackage();
    public RefPackage refOutermostPackage();
    public String refMofId();
    public Collection refVerifyConstraints(boolean deepVerify);
    public boolean equals(Object other);
    public int hashCode();
}
