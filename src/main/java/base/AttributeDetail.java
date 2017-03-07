package base;

/**
 * Created by mczal on 22/02/17.
 */
public class AttributeDetail {

  private String value;

  private int count;

  public AttributeDetail(String value, int count) {
    this.value = value;
    this.count = count;
  }

  public AttributeDetail(String value) {
    this.value = value;
  }

  public void addCountWith(int addition) {
    this.count += addition;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "AttributeDetail{" +
        "value='" + value + '\'' +
        ", count=" + count +
        '}';
  }

}
