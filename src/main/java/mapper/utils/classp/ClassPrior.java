package mapper.utils.classp;

import base.AttributeMetaInfo;
import base.TypeModel;

/**
 * Created by mczal on 22/02/17.
 */
public class ClassPrior extends AttributeMetaInfo<ClassPriorDetail> {

  public ClassPrior(String name) {
    super(name, TypeModel.CLASS);
  }

  @Override
  public String toString() {
    return "ClassPrior{} " + super.toString();
  }
}
