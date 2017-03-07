package mapper.utils.predictor;

import base.AttributeMetaInfo;
import base.TypeModel;
import java.util.HashMap;
import java.util.Map;
import mapper.utils.classp.ClassPrior;

/**
 * Created by mczal on 22/02/17.
 */
public class Predictor extends AttributeMetaInfo<PredictorDetail> {

  private Map<String, ClassPrior> classPriorMap = new HashMap<>();

  public Predictor(String name, TypeModel typeModel) {
    super(name, typeModel);
  }

  public Predictor(String name, TypeModel typeModel, double sigma, double mean) {
    super(name, typeModel, sigma, mean);
  }

  public Map<String, ClassPrior> getClassPriorMap() {
    return classPriorMap;
  }


}
