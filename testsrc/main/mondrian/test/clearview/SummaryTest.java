package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

public class SummaryTest extends ClearViewBase {
	
	public SummaryTest() {
		super();
	}
	
	public SummaryTest(String name) {
		super(name);
	}

	protected DiffRepository getDiffRepos() {
		return getDiffReposStatic();
	}
	
	private static DiffRepository getDiffReposStatic() {
		return DiffRepository.lookup(SummaryTest.class);
	}
	
	public static TestSuite suite() {
		return constructSuite(getDiffReposStatic(), SummaryTest.class);
	}

}
