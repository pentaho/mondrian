package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

public class TopBottomTest extends ClearViewBase {
	
	public TopBottomTest() {
		super();
	}
	
	public TopBottomTest(String name) {
		super(name);
	}

	protected DiffRepository getDiffRepos() {
		return getDiffReposStatic();
	}
	
	private static DiffRepository getDiffReposStatic() {
		return DiffRepository.lookup(TopBottomTest.class);
	}
	
	public static TestSuite suite() {
		return constructSuite(getDiffReposStatic(), TopBottomTest.class);
	}

}
