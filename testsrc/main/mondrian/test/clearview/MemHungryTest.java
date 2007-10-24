package mondrian.test.clearview;

import junit.framework.TestSuite;
import junit.framework.Test;
import mondrian.test.DiffRepository;

/**
 * TODO:  Even the humblest classes deserve comments.
 *
 * @author Khanh Vu
 * @version $Id$
 */
public class MemHungryTest extends ClearViewBase {

    public MemHungryTest() {
        super();
    }

    public MemHungryTest(String name) {
        super(name);
    }

    public DiffRepository getDiffRepos() {
        return getDiffReposStatic();
    }

    private static DiffRepository getDiffReposStatic() {
        return DiffRepository.lookup(MemHungryTest.class);
    }

    public static TestSuite suite() {
        return constructSuite(getDiffReposStatic(), MemHungryTest.class);
    }
}
// End MemHungryTest.java
