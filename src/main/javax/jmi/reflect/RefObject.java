package javax.jmi.reflect;

public interface RefObject extends RefFeatured {
    public boolean refIsInstanceOf(RefObject objType, boolean considerSubtypes);
    public RefClass refClass();
    public RefFeatured refImmediateComposite();
    public RefFeatured refOutermostComposite();
    public void refDelete();
}
