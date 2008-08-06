/*
// $Id$Id: //open/mondrian/src/main/mondrian/rolap/RolapCubeUsages.java#3 $
 */

public class RolapCubeUsages {
    private MondrianDef.CubeUsages cubeUsages;

    public RolapCubeUsages(MondrianDef.CubeUsages cubeUsage) {
        this.cubeUsages = cubeUsage;
    }

    public boolean shouldIgnoreUnrelatedDimensions(String baseCubeName) {
        if(cubeUsages==null || cubeUsages.cubeUsages == null){
            return false;
        }
        for (MondrianDef.CubeUsage usage : cubeUsages.cubeUsages) {
            if(usage.cubeName.equals(baseCubeName)
                && Boolean.TRUE.equals(usage.ignoreUnrelatedDimensions)) {
                return true;
            }
        }
        return false;
    }
}

// End RolapCubeUsages.java
