import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class URLReputationDecideMapper extends Mapper<Object, Text, Text, Text>
{
   private static double bias;
   private static DoubleWritable[] weights;
   
   @Override
   protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException
   {
      String str = context.getConfiguration().get("weights");
      String[] strWeights = str.split(",");
      //List<Double> weightVector = URLReputationClassifier.getOldWeightVector(); 
      int oldWeightSize = strWeights.length;
      weights = new DoubleWritable[oldWeightSize];
      for(int count = 0; count<oldWeightSize; count++) {
         weights[count] = new DoubleWritable(Double.parseDouble(strWeights[count]));
      }
      bias = Double.parseDouble(context.getConfiguration().get("bias"));
   }

   @Override
   protected void map(Object key, Text value, Context context) throws IOException,InterruptedException
   {
      Text emitKey;
      Text emitValue;
      String actualLabel;
      String predictedLabel;
      Map<Integer, Double> featureVector = new HashMap<Integer, Double>();
      String[] featureWeightSplit;
      double sigmoidValue;
      
      String[] split = value.toString().split(" ");
      int splitLen = split.length;
      actualLabel = split[0];
      emitKey = new Text(actualLabel);
      
      for(int i = 1; i<splitLen; i++) {
         featureWeightSplit = split[i].split(":");
         featureVector.put(Integer.parseInt(featureWeightSplit[0]), Double.parseDouble(featureWeightSplit[1]));
      }
      
      sigmoidValue = getSigmoidFunctionValue(featureVector);
      System.out.println(sigmoidValue);
      predictedLabel = "-1";
      if(sigmoidValue >=0.5) {
         predictedLabel = "+1";
      }
      emitValue = new Text(predictedLabel);
      
      manageCounters(actualLabel, predictedLabel, context);
      context.write(emitKey, emitValue);
   }
   
   private void manageCounters(String actualLabel, String predictedLabel, Context context) {
      if(actualLabel.equals("+1")) {
         context.getCounter(JobCounters.NO_OF_POSITIVE_EXAMPLES).increment(1l);
         if(!actualLabel.equals(predictedLabel)) {
            context.getCounter(JobCounters.NO_OF_INCORRECT_NEGATIVE_EXAMPLES).increment(1l);
         }
      } else {
         context.getCounter(JobCounters.NO_OF_NEGATIVE_EXAMPLES).increment(1l);
         if(!actualLabel.equals(predictedLabel)) {
            context.getCounter(JobCounters.NO_OF_INCORRECT_POSITIVE_EXAMPLES).increment(1l);
         }
      }
      
   }
   
   private static double getSigmoidFunctionValue(Map<Integer, Double> featureVector) {
      double e = Math.exp(-getWeightTX(featureVector) - bias);
      double t = 1.0;
      double res1 = t + e;
      double res = 1.0/res1;
      return res;
   }
   
   private static double getWeightTX(Map<Integer, Double> featureVector) {
      double result = 0;
      double currentWeight;
      for(Integer wordIndex: featureVector.keySet()) {
         currentWeight = weights[wordIndex].get();         
         result = result + currentWeight;
      }
      return result;
   }
}
