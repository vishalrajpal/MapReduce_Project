

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Strings;

public class URLReputationReducer extends Reducer<DoubleWritable, DoubleArrayWritable, DoubleWritable, DoubleArrayWritable>
{
   private double outputBias;
   private DoubleWritable[] outputWeight;
   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException
   {
      String str = context.getConfiguration().get("weights");
      String[] strWeights = str.split(",");
      //List<Double> weightVector = URLReputationClassifier.getOldWeightVector(); 
      int oldWeightSize = strWeights.length;
      outputWeight = new DoubleWritable[oldWeightSize];
      for(int count = 0; count<oldWeightSize; count++) {
         outputWeight[count] = new DoubleWritable(Double.parseDouble(strWeights[count]));
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
      StringBuilder sb = new StringBuilder();
      for(int counter = 0; counter<splitLen; counter++) {
          sum = sum + outputWeight[counter].get();
      }
      
      for(int counter = 0; counter<splitLen; counter++) {
         val = outputWeight[counter].get()/sum;
         outputWeight[counter].set(val);
         sb.append(val);
         if(counter != splitLen - 1) {
            sb.append(",");
         }
         //URLReputationClassifier.oldWeightVector.set(counter, URLReputationClassifier.oldWeightVector.get(counter) + val);
      }
      outputBias = outputBias/sum;
      //URLReputationClassifier.oldBias = outputBias;
      DoubleArrayWritable a = new DoubleArrayWritable(outputWeight);   
      context.getConfiguration().set("weights", sb.toString());
      context.getConfiguration().set("bias", String.valueOf(outputBias));
      context.write(new DoubleWritable(outputBias), a);
   }
   
}
