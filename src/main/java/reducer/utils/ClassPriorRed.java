package reducer.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mczal on 07/03/17.
 */
public class ClassPriorRed {

  private String name;

  private List<String> values = new ArrayList<String>();

  public ClassPriorRed(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getValues() {
    return values;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  @Override
  public String toString() {
    return "ClassPriorRed{" +
        "name='" + name + '\'' +
        ", values=" + values +
        '}';
  }

}
