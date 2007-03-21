package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

public class SubTotalTest extends ClearViewBase {
	
	public SubTotalTest() {
		super();
	}
	
	public SubTotalTest(String name) {
		super(name);
	}

	protected DiffRepository getDiffRepos() {
		return getDiffReposStatic();
	}
	
	private static DiffRepository getDiffReposStatic() {
		return DiffRepository.lookup(SubTotalTest.class);
	}
	
	public static TestSuite suite() {
		return constructSuite(getDiffReposStatic(), SubTotalTest.class);
	}

}
