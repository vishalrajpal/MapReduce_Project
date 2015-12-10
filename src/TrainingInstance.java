
import java.util.HashMap;
import java.util.Map;

public class TrainingInstance
{
   private Map<Integer, Double> featureVector;
   private int label;
   public TrainingInstance(String instanceStr) {
      featureVector = new HashMap<>();
      String[] split = instanceStr.split(" ");
      String labelStr = split[0];
      initializeLabel(labelStr);
      initializeFeatureVector(split);
   }
   
   private void initializeLabel(String labelString) {
      this.label = 1;
      if(labelString.equals("-1")) {
         this.label = 0;
      }
   }
   
   private void initializeFeatureVector(String[] featureSplit) {
      int splitLen = featureSplit.length;
      String[] featureValueSplit;
      for(int featureCounter = 1; featureCounter<splitLen; featureCounter++) {
         featureValueSplit = featureSplit[featureCounter].split(":");
         featureVector.put(Integer.parseInt(featureValueSplit[0]), Double.parseDouble(featureValueSplit[1]));
      }
   }

   public Map<Integer, Double> getFeatureVector() {
      return featureVector;
   }

   public int getLabel() {
      return label;
   }   
}
