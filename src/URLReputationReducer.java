

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class URLReputationReducer extends Reducer<DoubleWritable, DoubleArrayWritable, DoubleWritable, DoubleArrayWritable>
{
   private double outputBias;
   private DoubleWritable[] outputWeight;
   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException
   {
      List<Double> weightVector = URLReputationClassifier.getOldWeightVector(); 
      int oldWeightSize = weightVector.size();
      outputWeight = new DoubleWritable[oldWeightSize];
      for(int count = 0; count<oldWeightSize; count++) {
         outputWeight[count] = new DoubleWritable(weightVector.get(count));
      }
   }

   @Override
   protected void reduce(DoubleWritable mapKey, Iterable<DoubleArrayWritable> mapValue, Context context) throws IOException, InterruptedException
   {
      outputBias += mapKey.get();
      int oldWeightSize = outputWeight.length;      
      for(DoubleArrayWritable currentArrayWritable: mapValue) {
         DoubleWritable[] currentWeightVector = currentArrayWritable.get();
         for(int count = 0; count<oldWeightSize; count++) {
            outputWeight[count].set(outputWeight[count].get() + currentWeightVector[count].get());
         }
      }
   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException
   {
      int splitLen = outputWeight.length;
      double sum = outputBias;
      double val;
      for(int counter = 0; counter<splitLen; counter++) {
          sum = sum + outputWeight[counter].get();
      }
      
      for(int counter = 0; counter<splitLen; counter++) {
         val = outputWeight[counter].get()/sum;
         outputWeight[counter].set(val);
         URLReputationClassifier.oldWeightVector.set(counter, URLReputationClassifier.oldWeightVector.get(counter) + val);
      }
      outputBias = outputBias/sum;
      URLReputationClassifier.oldBias = outputBias;
      DoubleArrayWritable a = new DoubleArrayWritable(outputWeight);         
      context.write(new DoubleWritable(outputBias), a);
   }
   
}
