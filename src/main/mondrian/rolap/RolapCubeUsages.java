/*
// $Id$Id: //open/mondrian-release/lagunitas/src/main/mondrian/rolap/RolapCubeUsages.java#2 $
 */

public class RolapCubeUsages {
    private MondrianDef.CubeUsages cubeUsages;

    public RolapCubeUsages(MondrianDef.CubeUsages cubeUsage) {
        Util.deprecated("obsolete", false);
        this.cubeUsages = cubeUsage;
    }

    public boolean shouldIgnoreUnrelatedDimensions(
        RolapMeasureGroup measureGroup)
    {
        if (cubeUsages == null || cubeUsages.cubeUsages == null) {
            return false;
        }
        for (MondrianDef.CubeUsage usage : cubeUsages.cubeUsages) {
            if (usage.cubeName.equals(measureGroup.getName())
                && Boolean.TRUE.equals(usage.ignoreUnrelatedDimensions))
            {
                return true;
            }
        }
        return false;
    }
}

// End RolapCubeUsages.java
