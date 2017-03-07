package mapper.utils.classp;

import base.AttributeDetail;

/**
 * Created by mczal on 22/02/17.
 */
public class ClassPriorDetail extends AttributeDetail {

  private double sigma = -1;

  private double mean = -1;

  public ClassPriorDetail(String value, int count) {
    super(value, count);
  }

  public ClassPriorDetail(String value) {
    super(value);
  }

  public ClassPriorDetail(String value, int count, double mean, double sigma) {
    super(value, count);
    this.sigma = sigma;
    this.mean = mean;
  }

  @Override
  public String toString() {
    return "ClassPriorDetail{" +
        "sigma=" + sigma +
        ", mean=" + mean +
        "} " + super.toString();
  }

  public double getSigma() {
    return sigma;
  }

  public void setSigma(double sigma) {
    this.sigma = sigma;
  }

  public double getMean() {
    return mean;
  }

  public void setMean(double mean) {
    this.mean = mean;
  }

}
