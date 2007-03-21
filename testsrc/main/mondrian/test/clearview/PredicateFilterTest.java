package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

public class PredicateFilterTest extends ClearViewBase {
	
	public PredicateFilterTest() {
		super();
	}
	
	public PredicateFilterTest(String name) {
		super(name);
	}

	protected DiffRepository getDiffRepos() {
		return getDiffReposStatic();
	}
	
	private static DiffRepository getDiffReposStatic() {
		return DiffRepository.lookup(PredicateFilterTest.class);
	}
	
	public static TestSuite suite() {
		return constructSuite(getDiffReposStatic(), PredicateFilterTest.class);
	}

}
