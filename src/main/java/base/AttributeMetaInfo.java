package base;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mczal on 22/02/17.
 */
public class AttributeMetaInfo<T extends AttributeDetail> {

  private String name;

  private TypeModel typeModel;

  private double sigma;

  private double mean;

  private Map<String, T> attrDetailMap = new HashMap<String, T>();

  public AttributeMetaInfo(String name, TypeModel typeModel) {
    this.name = name;
    this.typeModel = typeModel;
  }

  public AttributeMetaInfo(String name, TypeModel typeModel, double sigma, double mean) {
    this.name = name;
    this.typeModel = typeModel;
    this.sigma = sigma;
    this.mean = mean;
  }

  public double getSigma() {
    return sigma;
  }

  public double getMean() {
    return mean;
  }

  public TypeModel getTypeModel() {
    return typeModel;
  }

  public Map<String, T> getAttrDetailMap() {
    return attrDetailMap;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "AttributeMetaInfo{" +
        "name='" + name + '\'' +
        ", typeModel=" + typeModel +
        ", sigma=" + sigma +
        ", mean=" + mean +
        ", attrDetailMap=" + attrDetailMap +
        '}';
  }
}
