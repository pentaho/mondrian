package mondrian.rolap;

/**
 * Gives access to protected members, for testing purposes.
 *
 * <p>Methods in this class are subject to change without notice.
 *
 * @author jhyde
 * @version $Id$
 */
public class RolapTrojan {
    public static final RolapTrojan INSTANCE = new RolapTrojan();

    private RolapTrojan() {
    }

    public RolapSchema.PhysExpr getAttributeNameExpr(RolapAttribute attribute) {
        return attribute.nameExp;
    }
}

// End RolapTrojan.java
