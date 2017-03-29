package mapper.utils.predictor;

import base.AttributeDetail;
import java.util.HashMap;
import java.util.Map;
import mapper.utils.classp.ClassPrior;
import mapper.utils.classp.ClassPriorDetail;

/**
 * Created by mczal on 22/02/17.
 */
public class PredictorDetail extends AttributeDetail {

  private Map<String, ClassPrior> classPriorMap = new HashMap<String, ClassPrior>();

  public Map<String, ClassPrior> getClassPriorMap() {
    return classPriorMap;
  }

  public PredictorDetail(String value, int count) {
    super(value, count);
//    ClassPriorDetail classPriorDetail = classPrior.getAttrDetailMap().get(classPriorValue);
//    if (classPriorDetail == null) {
//      /**
//       * -1 FOR UNUSED
//       * */
//      classPriorDetail = new ClassPriorDetail(classPriorValue, -1);
//    }
//    classPrior.getAttrDetailMap().put(classPriorValue, classPriorDetail);
  }
}
