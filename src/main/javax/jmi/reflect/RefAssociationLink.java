package javax.jmi.reflect;

public interface RefAssociationLink {
    public RefObject refFirstEnd();
    public RefObject refSecondEnd();
    public boolean equals(Object other);
    public int hashCode();
}

