package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

public class MetricFilterTest extends ClearViewBase {
	
	public MetricFilterTest() {
		super();
	}
	
	public MetricFilterTest(String name) {
		super(name);
	}

	protected DiffRepository getDiffRepos() {
		return getDiffReposStatic();
	}
	
	private static DiffRepository getDiffReposStatic() {
		return DiffRepository.lookup(MetricFilterTest.class);
	}
	
	public static TestSuite suite() {
		return constructSuite(getDiffReposStatic(), MetricFilterTest.class);
	}

}
