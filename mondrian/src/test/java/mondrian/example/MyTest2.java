package mondrian.example;

import org.junit.jupiter.api.Test;

import mondrian.junit5.FastFoodmardDataLoader;
import mondrian.junit5.MondrianRuntimeSupport;

@MondrianRuntimeSupport(dataLoader = FastFoodmardDataLoader.class)
public class MyTest2 {

    @Test
    void testName() throws Exception {
	System.out.println(1);
    }
}
