package mapper;

import base.TypeInfo;
import base.TypeModel;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import mapper.utils.classp.ClassContainer;
import mapper.utils.classp.ClassPrior;
import mapper.utils.classp.ClassPriorDetail;
import mapper.utils.predictor.Predictor;
import mapper.utils.predictor.PredictorContainer;
import mapper.utils.predictor.PredictorDetail;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mczal on 22/02/17.
 */
public class TestingMapper extends Mapper<Object, Text, Text, Text> {

  private static final String HDFS_AUTHORITY = "hdfs://localhost:9000";

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final IntWritable one = new IntWritable(1);
  private ClassContainer classContainer = new ClassContainer();
  private PredictorContainer predictorContainer = new PredictorContainer();
  private String[] classSplitConf;
  private String[] attrSplitConf;

  @Override
  protected void map(Object key, Text value, final Context context)
      throws IOException, InterruptedException {
    String[] in = value.toString().trim().split(",");

    /**
     * Fixed result for each class
     * allResults format :
     *      [ClassName|ClassVal|Result|InputClassValue]
     * */
    List<String> allResults = new ArrayList<String>();
    for (int i = 0; i < classSplitConf.length; i++) {
      int classIdx = Integer.parseInt(classSplitConf[i].trim().split(",")[1].trim());
      String className = classSplitConf[i].trim().split(",")[0].trim().toLowerCase();

      String currInClassValue = in[classIdx].trim().toLowerCase();

      ClassPrior sumClassPrior = classContainer.getClassPriorMap().get(className);
      if (sumClassPrior == null) {
        throw new IllegalArgumentException(
            "SUM_CLASS_PRIOR IS NULL on line 48 : TestingMapper.class \n"
                + "for: " + className + "\n"
                + "from: " + sumClassPrior);
      }

      /**
       * Checking for every value of current class and get the greatest
       * currPredictorResult format :
       *      [ClassName|ClassVal|Result|InputClassValue]
       * */
      List<String> allClassResult = new ArrayList<String>();

      for (Iterator<Entry<String, ClassPriorDetail>> p = sumClassPrior.getAttrDetailMap().entrySet()
          .iterator(); p.hasNext(); ) {
        ClassPriorDetail forSumClassPriorDetail = p.next().getValue();

        double currClassAllPredictorResult = 1.0;
        int flag = 0;
        outer:
        for (int j = 0; j < attrSplitConf.length; j++) {
          int attrIdx = Integer.parseInt(attrSplitConf[j].trim().split(",")[1]);
          String attrName = attrSplitConf[j].trim().split(",")[0].trim().toLowerCase();
          String attrType = attrSplitConf[j].trim().split(",")[2].trim().toLowerCase();

          String currInAttrValue = in[attrIdx].trim().toLowerCase();

          if (attrType.equalsIgnoreCase(TypeInfo.DISCRETE.name())) {

            Predictor pred = predictorContainer.getPredictorMap().get(attrName);
            if (pred == null) {
              throw new IllegalArgumentException("PRED IS NULL ON line 93 : TestingMapper.class");
            }
            PredictorDetail predictorDetail = pred.getAttrDetailMap().get(currInAttrValue);
            if (predictorDetail == null) {
              throw new IllegalArgumentException(
                  "PRED_DETAIL IS NULL ON line 97 : TestingMapper.class\n For Val="
                      + currInAttrValue + "\n"
                      + "From: " + pred.getAttrDetailMap().toString());
            }
            ClassPrior classPrior = predictorDetail.getClassPriorMap().get(className);
//            ClassPrior classPrior = classContainer.getClassPriorMap().get(className);
            if (classPrior == null) {
              throw new IllegalArgumentException(
                  "CLASS_PRIOR IS NULL ON line 104 : TestingMapper.class" + " => " + predictorDetail
                      .toString());
            }

            ClassPriorDetail classPriorDetail = classPrior.getAttrDetailMap()
                .get(forSumClassPriorDetail.getValue());
            if (classPriorDetail == null) {
              /**
               * TODO: ZERO FREQ HANDLING
               * */
              String from = classPrior.getAttrDetailMap().toString();
              throw new RuntimeException(
                  "Zero frequency problem occured for: ,predictorName=" + pred.getName()
                      + ",predVal=" + predictorDetail.getValue() + ",classPrior=" + classPrior
                      .getName() + ",classPriorDetail=" + forSumClassPriorDetail.getValue()
                      + ". Will Skip Attribute [" + pred.getName() + "]\n\n"
                      + "From: classPrior.getAttrDetailMap() -> " + from);
//              flag = 1;
//              break outer;

//              continue;
//              throw new IllegalArgumentException(
//                  "Class Name = " + classPrior.getName() + "\n" +
//                      "PredName = " + pred.getName() + "\n" +
//                      "PredDetailName = " + predictorDetail.getValue() + "\n" +
//                      "CLASS_PRIOR_DETAIL IS NULL ON line 89 : TestingMapper.class :: \nclassPrior.getAttrDetailMap()"
//                      + " => " + classPrior.getAttrDetailMap()
//                      + "\nforSumClassPriorDetail.getValue() => "
//                      + forSumClassPriorDetail.getValue());
            }
            int countDividend = classPriorDetail.getCount();
            /**
             * [START]
             * DEBUG SECTION
             * */
            ClassPrior divClassPrior = pred.getClassPriorMap().get(className);
            if (divClassPrior == null) {
              throw new IllegalArgumentException(
                  "DIV_CLASS_PRIOR IS NULL ON line 132 : TestingMapper.class :: \n" +
                      "pred.getClassPriorMap() => " + pred.getClassPriorMap().toString() + "\n" +
                      "className => " + className);
            }
            ClassPriorDetail divClassPriorDetail = divClassPrior.getAttrDetailMap()
                .get(forSumClassPriorDetail.getValue());
            if (divClassPriorDetail == null) {
              throw new IllegalArgumentException(
                  "DIV_CLASS_PRIOR_DETAIL IS NULL ON line 140 : TestingMapper.class :: \n" +
                      "divClassPrior.getAttrDetailMap() => " + divClassPrior.getAttrDetailMap()
                      .toString() + "\n" +
                      "currInAttrValue => " + forSumClassPriorDetail.getValue());
            }
            /**
             * [END]
             * DEBUG SECTION
             * */
            int divisor =
//                pred.getClassPriorMap().get(className).getAttrDetailMap()
//                .get(currInAttrValue)
                divClassPriorDetail.getCount();
            double currRes = (countDividend * 1.0) / (divisor * 1.0);
//            String print1 =
//                "MCZAL \ncurrClassAllPredictorResult BEFORE : " + currClassAllPredictorResult;

//
            currClassAllPredictorResult *= currRes;
//

//            String print2 = "\nMCZAL: \ndivisor: " + divisor + "\ncountDividend: " + countDividend;
//            String print3 = "\nMCZAL: \ncurrRes: " + currRes + " \ncurrClassAllPredictorResult: "
//                + currClassAllPredictorResult;
//            context.write(
//                new Text(
//                    "\n-----------------\nclassName => " + className + "\n | "
//                        + " forSumClassPriorDetail.getValue() => "
//                        + forSumClassPriorDetail.getValue() + "\n | currClassAllPredictorResult =>"
//                        + currClassAllPredictorResult + "\n | currInClassValue => "
//                        + currInClassValue + "\n\n" + print1 + print2
//                        + print3 + "\n-----------------\n"),
//                new Text());

          } else if (attrType.equals(TypeInfo.NUMERICAL.name().toLowerCase())) {
            /**
             * use currInAttrValue
             * */
            Predictor pred = predictorContainer.getPredictorMap().get(attrName);
            if (pred == null) {
              throw new IllegalArgumentException(
                  "PRED IS NULL ON line 176 : TestingMapper.class for:\nattrName='" + attrName
                      + "'\n"
                      + "from: " + predictorContainer.getPredictorMap().toString());
            }
            ClassPrior classPrior = pred.getClassPriorMap().get(className);
            if (classPrior == null) {
              throw new IllegalArgumentException(
                  "CLASS_PRIOR IS NULL ON line 183 : TestingMapper.class for:\nclassName='"
                      + className + "'\n"
                      + "from: " + pred.getClassPriorMap().toString());
            }
            ClassPriorDetail detail = classPrior.getAttrDetailMap()
                .get(forSumClassPriorDetail.getValue());
            if (detail == null) {
              throw new IllegalArgumentException(
                  "CLASS_DETAIL IS NULL ON line 191 : TestingMapper.class for:\nclassVal='"
                      + forSumClassPriorDetail.getValue() + "'\n"
                      + "from: " + classPrior.getAttrDetailMap().toString());
            }

            /**
             * Calculate Norm.Dist. =>
             * */
            String debugSecNum = "!!!!!!!!!!!!\n"
                + "For Class=" + forSumClassPriorDetail.getValue() + "\n";

            double mean = detail.getMean();
            double sigma = detail.getSigma();
            debugSecNum += "mean=" + mean + "\n"
                + "sigma=" + sigma + "\n";

            double divisor = Math.sqrt(2.0 * Math.PI * sigma);
            debugSecNum += "divisor=" + divisor + "\n";

            Double currNumericalInAttrValue;
            if (currInAttrValue.contains(".")) {
              currNumericalInAttrValue = Double.parseDouble(currInAttrValue);
            } else {
              currNumericalInAttrValue = Integer.parseInt(currInAttrValue) * 1.0;
            }
            double powerDividend = Math.pow((currNumericalInAttrValue - mean), 2) * -1;

            debugSecNum += "currNumericalInAttrValue=" + currNumericalInAttrValue + "\n";
            debugSecNum += "powerDividend=" + powerDividend + "\n";
//            double powerDividend = Math.pow((Double.parseDouble(currInAttrValue) - mean), 2) * -1;

            double powerDivisor = 2.0 * Math.pow(sigma, 2);

            debugSecNum += "powerDivisor=" + powerDivisor + "\n";

            double resPower = powerDividend / powerDivisor;

            debugSecNum += "resPower=" + resPower + "\n";

            double currRes = (1 / divisor) * (Math.pow(Math.E, resPower));

            debugSecNum += "currRes=" + currRes + "\n";
            debugSecNum += "!!!!!!!!!!!!";

//            context.write(new Text(debugSecNum), new Text());

//            String print1 = "+++++++++++++\n"
//                + "For: " + className + "=" + forSumClassPriorDetail.getValue() + "\n"
//                + "currInAttrValue=" + currInAttrValue + "\n"
//                + "currRes=" + currRes + "\n"
//                + "ResBefore=" + currClassAllPredictorResult + "\n";

            //
            currClassAllPredictorResult *= currRes;
            //

//            String print2 = "ResAfter=" + currClassAllPredictorResult + "\n"
//                + "+++++++++++++";
//            context.write(new Text(print1 + print2), new Text());

          } else {
            throw new IllegalArgumentException(
                "System Error : INVALID TYPE HERE => " + attrType + " != " + TypeInfo.DISCRETE
                    .name().toLowerCase() + " or " + TypeInfo.NUMERICAL.name().toLowerCase());
          }
        }

        if (flag == 0) {
          /**
           *  times with P(C=c) possibilities of current class prior
           * currPredRes *= (classInfoDetail.getCount() * 1.0) / (accFinal * 1.0);
           */
          ClassPrior classPrior = classContainer.getClassPriorMap().get(className);
          if (classPrior == null) {
            throw new IllegalArgumentException(
                "ClassPrior is null on line 221 className=" + className + "\n"
                    + "from: " + classContainer.getClassPriorMap().toString());
          }
          ClassPriorDetail detail = classPrior.getAttrDetailMap()
              .get(forSumClassPriorDetail.getValue());
          if (detail == null) {
            throw new IllegalArgumentException(
                "ClassPriorDetail is null on line 229. classVal=" + forSumClassPriorDetail
                    .getValue()
                    + "\n"
                    + "from: " + classPrior.getAttrDetailMap().toString());
          }
          double currClassCount = detail.getCount();
          double allCurrClassCount = 0.0;
          for (Entry<String, ClassPriorDetail> ent : classPrior
              .getAttrDetailMap().entrySet()) {
            allCurrClassCount += ent.getValue().getCount();
          }
          /**
           * [ClassName|ClassVal|Result|InputClassValue]
           * */
          currClassAllPredictorResult *= (currClassCount / allCurrClassCount);
          allClassResult.add(className + "|" + forSumClassPriorDetail.getValue() + "|"
              + currClassAllPredictorResult + "|" + currInClassValue);
        } else {
          throw new IllegalArgumentException("FLAG \nFLAG\nZero frequency problem occured.\n"
              + "Ignore for Class=" + sumClassPrior.getName() + " -> Value="
              + forSumClassPriorDetail.getValue());
        }
      }

      /**
       *
       * maxClass format :
       *      [ClassName|ClassVal|Result|InputClassValue]
       * */
      String maxClass = "";
      double checker = Double.MIN_VALUE;
      double divisorNorm = 0.0;
      /**
       * [ClassName|ClassVal|Result|InputClassValue]
       * */
      for (String s : allClassResult) {
//        context.write(new Text("\ns=" + s), new Text());
        double currentVal = Double.parseDouble(s.split("\\|")[2]);
        divisorNorm += currentVal;
//        context
//            .write(new Text("\n Is checker=" + checker + " < currVal=" + currentVal), new Text());
        if (checker < currentVal) {
//          context
//              .write(new Text("\nchecker=" + checker + " < currVal=" + currentVal + " ARE TRUE"),
//                  new Text());
          checker = currentVal;
          maxClass = s;
        }
      }
//      context.write(new Text(maxClass), new Text(checker + ""));
      double resNorm = (checker / divisorNorm) * 100;
      String[] splitter = maxClass.split("\\|");
      DecimalFormat df = new DecimalFormat("#.00");
      df.setRoundingMode(RoundingMode.HALF_UP);
      String maxResult =
          splitter[0] + "|" + "predicted=" + splitter[1] + "|percentage=" + df.format(resNorm)
              + "%"
              + "|"
              + "actual=" + splitter[3];
      allResults.add(maxResult);
    }
    for (String s : allResults) {
      String[] splitter = s.split("\\|");
      String outputKey = splitter[0];
      String outputVal = splitter[1] + "|" + splitter[2] + "|" + splitter[3];
      /**
       * TODO: FVKING
       * */
      context.write(new Text(outputKey), new Text(outputVal));

    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    /**
     * Getting conf
     * */
    classSplitConf = context.getConfiguration().get("classes").split(";");
    attrSplitConf = context.getConfiguration().get("attributes").split(";");

    String outputModelPath = conf.get("outputModelPath");
    int laplacianSmoothingAdder = Integer.parseInt(conf.get("laplacianSmoothingAdder"));

    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(HDFS_AUTHORITY + outputModelPath);
    FileStatus[] fileStatuses = fs.listStatus(path);
    for (int i = 0; i < fileStatuses.length; i++) {
      BufferedReader br = new BufferedReader(
          new InputStreamReader(fs.open(fileStatuses[i].getPath())));

      while (br.ready()) {
        String currLine = br.readLine().trim().toLowerCase();
        String[] currLineSplitter = currLine.split("\\|");

        String currType = currLineSplitter[currLineSplitter.length - 1].trim();
        int bayesianCount = 1;
        /**
         * IF CLASS
         * */
        if (currType.equalsIgnoreCase(TypeModel.CLASS.name().toLowerCase())) {
          String[] specSplitter = currLine.split("\\|")[0].split(",");
          ClassPrior classPrior = classContainer.getClassPriorMap().get(specSplitter[0].trim());
          if (classPrior == null) {
            classPrior = new ClassPrior(specSplitter[0].trim());
          }
          ClassPriorDetail classPriorDetail = new ClassPriorDetail(specSplitter[1].trim(),
              (int) Double.parseDouble(specSplitter[2].trim()));
          classPrior.getAttrDetailMap().put(specSplitter[1].trim(), classPriorDetail);
          classContainer.getClassPriorMap().put(specSplitter[0].trim(), classPrior);
        }
        /**
         * IF DISCRETE
         * */
        else if (currType.equalsIgnoreCase(TypeModel.DISCRETE.name().toLowerCase())) {
          String[] specSplitter = currLine.split("\\|")[0].split(",");
          Predictor predictor = predictorContainer.getPredictorMap().get(specSplitter[0].trim());
          if (predictor == null) {
            predictor = new Predictor(specSplitter[0], TypeModel.DISCRETE);
          }

          /**
           * [START]
           * Additional Section For SUM Count Predictor Of ClassPrior Value
           * */
          ClassPrior addClassPrior = predictor.getClassPriorMap().get(specSplitter[2].trim());
          if (addClassPrior == null) {
            addClassPrior = new ClassPrior(specSplitter[2].trim());
            predictor.getClassPriorMap().put(specSplitter[2].trim(), addClassPrior);
          }
          ClassPriorDetail addClassPriorDetail = addClassPrior.getAttrDetailMap()
              .get(specSplitter[3].trim());
          if (addClassPriorDetail == null) {
            addClassPriorDetail = new ClassPriorDetail(specSplitter[3].trim(),
                (int) Double.parseDouble(specSplitter[4]));
            addClassPrior.getAttrDetailMap().put(specSplitter[3].trim(), addClassPriorDetail);
          } else {
            addClassPriorDetail.addCountWith((int) Double.parseDouble(specSplitter[4]));
          }
          /**
           * [END]
           * */

          PredictorDetail predictorDetail = predictor.getAttrDetailMap()
              .get(specSplitter[1].trim());
          if (predictorDetail == null) {
            predictorDetail = new PredictorDetail(specSplitter[1].trim(),
                -1);
            predictor.getAttrDetailMap().put(specSplitter[1].trim(), predictorDetail);
          }
          ClassPrior classPrior = predictorDetail.getClassPriorMap().get(specSplitter[2].trim());
          if (classPrior == null) {
            classPrior = new ClassPrior(specSplitter[2].trim());
            predictorDetail.getClassPriorMap().put(specSplitter[2].trim(), classPrior);
          }
          ClassPriorDetail classPriorDetail = new ClassPriorDetail(specSplitter[3].trim(),
              (int) Double.parseDouble(specSplitter[4].trim()));
          classPrior.getAttrDetailMap().put(specSplitter[3].trim(), classPriorDetail);
          predictorContainer.getPredictorMap().put(specSplitter[0].trim(), predictor);
        }
        /**
         * IF NUMERIC
         * */
        else if (currType.equalsIgnoreCase(TypeModel.NUMERIC.name().toLowerCase())) {
          String[] metaSplitter = currLine.split(";");

          String[] specInfoSplitter = metaSplitter[0].split(",");
          String[] specDetailSplitter = metaSplitter[1].split("\\|");

          Predictor predictor = predictorContainer.getPredictorMap()
              .get(specInfoSplitter[0].trim());
          if (predictor == null) {
            predictor = new Predictor(specInfoSplitter[0].trim(), TypeModel.NUMERIC);
          }
          ClassPrior classPrior = predictor.getClassPriorMap().get(specInfoSplitter[1].trim());
          if (classPrior == null) {
            classPrior = new ClassPrior(specInfoSplitter[1].trim());
            predictor.getClassPriorMap().put(specInfoSplitter[1].trim(), classPrior);
          }
          ClassPriorDetail classPriorDetail = new ClassPriorDetail(specInfoSplitter[2].trim(), -1,
              Double.parseDouble(specDetailSplitter[0].trim()),
              Double.parseDouble(specDetailSplitter[1].trim()));
          classPrior.getAttrDetailMap().put(specInfoSplitter[2].trim(), classPriorDetail);
          predictorContainer.getPredictorMap().put(specInfoSplitter[0].trim(), predictor);
        }
        /**
         * ERROR
         * */
        else {
          throw new IllegalArgumentException(
              "ON LINE : 400 .\nNO TYPE : " + currType + " !!! VIOLATION ON MODELS");
        }
      }
    }
    this.laplacianSmoothing(laplacianSmoothingAdder);
  }

  private void laplacianSmoothing(int laplacianSmoothingAdder) {
    if (classContainer.getClassPriorMap().size() <= 0) {
      throw new RuntimeException("Class container size is equal to zero.");
    }
    classContainer.getClassPriorMap().forEach((className, classPrior) -> {
//      classContainer.getClassPriorMap().put(className, classPrior);
      /**
       * FOR EACH CLASS
       * */
      classPrior.getAttrDetailMap().forEach((classValue, classPriorDetail) -> {
//        classPrior.getAttrDetailMap().put(classValue, classPriorDetail);
        /**
         * FOR EACH CLASS VALUE
         * */
        if (predictorContainer.getPredictorMap().size() <= 0) {
          throw new RuntimeException("Predictor container size is equal to zero.");
        }
        predictorContainer.getPredictorMap().forEach((predictorName, predictor) -> {
//          predictorContainer.getPredictorMap().put(predictorName, predictor);
          AtomicInteger totalAdditionForCurrClassDetail = new AtomicInteger(0);
          /**
           * FOR EACH PREDICTOR
           * */
          predictor.getAttrDetailMap().forEach((predictorDetailName, predictorDetail) -> {
//            predictor.getAttrDetailMap().put(predictorDetailName, predictorDetail);
            /**
             * FOR EACH PREDICTOR DETAIL
             * DO ADDITION FOR AVAILABLE ATTR AND CREATE NEW IF NOT AVAILABLE
             * */
            /**
             * CHECKER METHOD
             * */
            ClassPriorDetail classPriorDetailModify = predictorDetail.getClassPriorMap()
                .get(className)
                .getAttrDetailMap()
                .get(classValue);
            if (classPriorDetailModify == null) {
              classPriorDetailModify = new ClassPriorDetail(classValue, laplacianSmoothingAdder);
              predictorDetail.getClassPriorMap().get(className).getAttrDetailMap()
                  .put(classValue, classPriorDetailModify);
              ClassPriorDetail tmpClassPriorDetail = classPrior.getAttrDetailMap().get(classValue);
              tmpClassPriorDetail
                  .setCount(tmpClassPriorDetail.getCount() + laplacianSmoothingAdder);
              /**
               * ----------------- WORK-SEPARATOR (UNUSED)-----------------
               * */

            } else {
              classPriorDetailModify
                  .setCount(classPriorDetailModify.getCount() + laplacianSmoothingAdder);
              /**
               * ----------------- WORK-SEPARATOR (UNUSED) -----------------
               * */

            }
            /**
             * HANDLER FOR CLASS PRIOR
             * */
            for (int i = 0; i < laplacianSmoothingAdder; i++) {
              totalAdditionForCurrClassDetail.incrementAndGet();
            }
          });
          ClassPriorDetail classPriorDetailSecondHandler = predictor.getClassPriorMap()
              .get(className)
              .getAttrDetailMap().get(classValue);
          classPriorDetailSecondHandler
              .setCount(
                  classPriorDetailSecondHandler.getCount() + totalAdditionForCurrClassDetail.get());
        });
      });
    });
  }

}
