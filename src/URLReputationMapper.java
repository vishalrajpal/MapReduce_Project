

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class URLReputationMapper extends Mapper<Object, Text, DoubleWritable, DoubleArrayWritable>
{
   private List<TrainingInstance> instances;
   private double bias;
   private double learningRate;
   private DoubleWritable[] weights;
   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException
   {
      instances = new ArrayList<>();
      List<Double> weightVector = URLReputationClassifier.getOldWeightVector(); 
      int oldWeightSize = weightVector.size();
      weights = new DoubleWritable[oldWeightSize];
      for(int count = 0; count<oldWeightSize; count++) {
         weights[count] = new DoubleWritable(weightVector.get(count));
      }
      bias = URLReputationClassifier.getOldBias();
      learningRate = 0.5;
   }

   @Override
   protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
   {
      TrainingInstance newInstance = new TrainingInstance(value.toString());
      instances.add(newInstance);
   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException
   {
      int noOfTrainingInstances = instances.size();
      int randomTrainingIndex;
      TrainingInstance randomInstance;
      Map<Integer, Double> currentFeatureVector;
      double sigmoidFunctionValue;
      double delta;
      for(int count = 0; count <  noOfTrainingInstances; count++) {
         randomTrainingIndex = (int)(Math.random() * noOfTrainingInstances);
         randomInstance = instances.get(randomTrainingIndex);
         currentFeatureVector = randomInstance.getFeatureVector();
         sigmoidFunctionValue = getSigmoidFunctionValue(currentFeatureVector);
         delta = randomInstance.getLabel() - sigmoidFunctionValue;
         addDeltaToWeightsVector(delta, currentFeatureVector);
      }
      normalizeWeights();
      DoubleArrayWritable a = new DoubleArrayWritable(weights);
      context.write(new DoubleWritable(bias), a);
   }
   
   private double getSigmoidFunctionValue(Map<Integer, Double> featureVector) {
      double e = Math.exp(-getWeightTX(featureVector) - bias);
      double t = 1.0;
      double res1 = t + e;
      double res = 1.0/res1;
      return res;
   }
   
   private double getWeightTX(Map<Integer, Double> featureVector) {
      double result = 0;
      double currentWeight;
      for(Integer wordIndex: featureVector.keySet()) {
         currentWeight = weights[wordIndex].get();
         result = result + currentWeight;
      }
      return result;
   }
   
   private void addDeltaToWeightsVector(double delta, Map<Integer, Double> featureVector) {
      double prod = (learningRate * delta);
      bias = bias + prod;
      int key;
      for(Map.Entry<Integer, Double> feature: featureVector.entrySet()) {
         key = feature.getKey();
         weights[key].set(weights[key].get() + (prod * feature.getValue()));
      }
   }
   
   private void normalizeWeights() {
      Double sum = bias;
      for(DoubleWritable weight : weights) {
         sum += weight.get();
      }
      if(sum==0.0) {
         return;
      }
      bias = bias/sum;
      Double val;
      int noOfAttributes = weights.length;
      for(int weightCounter = 0; weightCounter<noOfAttributes; weightCounter++) {
         val = weights[weightCounter].get()/sum;
         weights[weightCounter].set(val);
      }
   }
}
