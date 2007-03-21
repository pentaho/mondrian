package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

public class GrandTotalTest extends ClearViewBase {
	
	public GrandTotalTest() {
		super();
	}
	
	public GrandTotalTest(String name) {
		super(name);
	}

	protected DiffRepository getDiffRepos() {
		return getDiffReposStatic();
	}
	
	private static DiffRepository getDiffReposStatic() {
		return DiffRepository.lookup(GrandTotalTest.class);
	}
	
	public static TestSuite suite() {
		return constructSuite(getDiffReposStatic(), GrandTotalTest.class);
	}

}
