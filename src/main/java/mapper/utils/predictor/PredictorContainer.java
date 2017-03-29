package mapper.utils.predictor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mczal on 22/02/17.
 */
public class PredictorContainer {

  private Map<String, Predictor> predictorMap = new HashMap<String, Predictor>();

  public Map<String, Predictor> getPredictorMap() {
    return predictorMap;
  }

  public void setPredictorMap(
      Map<String, Predictor> predictorMap) {
    this.predictorMap = predictorMap;
  }

  @Override
  public String toString() {
    return "PredictorContainer{" +
        "predictorMap=" + predictorMap +
        '}';
  }
}
