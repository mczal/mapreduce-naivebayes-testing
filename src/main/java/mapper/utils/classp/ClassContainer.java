package mapper.utils.classp;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mczal on 22/02/17.
 */
public class ClassContainer {

  private Map<String, ClassPrior> classPriorMap = new HashMap<String, ClassPrior>();

  public Map<String, ClassPrior> getClassPriorMap() {
    return classPriorMap;
  }

  public void setClassPriorMap(Map<String, ClassPrior> classPriorMap) {
    this.classPriorMap = classPriorMap;
  }

  @Override
  public String toString() {
    return "ClassContainer{" +
        "classPriorMap=" + classPriorMap +
        '}';
  }
}
