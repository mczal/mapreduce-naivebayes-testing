package reducer.utils;

import java.util.HashMap;

/**
 * Created by mczal on 07/03/17.
 */
public class ClassContainerRed {

  private HashMap<String, ClassPriorRed> classPriorRedHashMap = new HashMap<String, ClassPriorRed>();

  public HashMap<String, ClassPriorRed> getClassPriorRedHashMap() {
    return classPriorRedHashMap;
  }

  public void setClassPriorRedHashMap(
      HashMap<String, ClassPriorRed> classPriorRedHashMap) {
    this.classPriorRedHashMap = classPriorRedHashMap;
  }

}
