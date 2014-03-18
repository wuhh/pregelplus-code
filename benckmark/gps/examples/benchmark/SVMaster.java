package gps.examples.benchmark;
import gps.globalobjects.BooleanANDGlobalObject;
import gps.graph.Master;


/**
 * Master class for the extended sssp algorithm. It coordinates the flow of the
 * two stages of the algorithm as well as computing the average distance at the
 * very end of the computation.
 * 
 * @author semihsalihoglu
 */
public class SVMaster extends Master {
    @Override
    public void compute(int superstepNo) {	
	getGlobalObjectsMap().clearNonDefaultObjects();
	getGlobalObjectsMap().putGlobalObject("Star", new BooleanANDGlobalObject(false));
	
    }

}