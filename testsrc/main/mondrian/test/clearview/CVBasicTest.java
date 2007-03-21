package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

public class CVBasicTest extends ClearViewBase {
	
	public CVBasicTest() {
		super();
	}
	
	public CVBasicTest(String name) {
		super(name);
	}

	protected DiffRepository getDiffRepos() {
		return getDiffReposStatic();
	}
	
	private static DiffRepository getDiffReposStatic() {
		return DiffRepository.lookup(CVBasicTest.class);
	}
	
	public static TestSuite suite() {
		return constructSuite(getDiffReposStatic(), CVBasicTest.class);
	}

}
