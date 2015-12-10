
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class URLReputationClassifier
{
   protected static List<Double> oldWeightVector;
   protected static double oldBias;
   
   public static List<Double> getOldWeightVector() {
      return oldWeightVector;
   }

   public static double getOldBias() {
      return oldBias;
   }
   
   private static void train(String[] args) {
      try
      {
         int noOfWords = 3231961;
         oldWeightVector = new ArrayList<Double>(Collections.nCopies(noOfWords, (double)0.0));
         oldBias = 0.0;
         for(int i = 1; i<=10; i++) {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "URLReputation");
            job.setJarByClass(URLReputationClassifier.class);
            job.setMapperClass(URLReputationMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleArrayWritable.class);
            job.setReducerClass(URLReputationReducer.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(DoubleArrayWritable.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]+i));
            if(!job.waitForCompletion(true)) {
               System.exit(1);
            } else {
               System.out.println(i);              
            }
         }
      }
      catch (Exception e)
      {
         System.out.print("Unable to train");
         e.printStackTrace();
      }
   }
   
   public static void predict(String[] args) {
      System.out.println("In predict");
      //normalizeWeights();
      try
      {
         Configuration conf = new Configuration();
         Job job = new Job(conf, "URLReputationPrediction");
         job.setJarByClass(URLReputationClassifier.class);
         job.setMapperClass(URLReputationDecideMapper.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(Text.class);
         job.setReducerClass(URLReputationDecideReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         job.setNumReduceTasks(1);
         FileInputFormat.addInputPath(job, new Path(args[1]));
         FileOutputFormat.setOutputPath(job, new Path(args[3]));
         if(!job.waitForCompletion(true)) {
            System.exit(1);
         } else {
            Counters counters = job.getCounters();
            long noOfPositiveExamples = counters.findCounter(JobCounters.NO_OF_POSITIVE_EXAMPLES).getValue();
            long noOfNegativeExamples = counters.findCounter(JobCounters.NO_OF_NEGATIVE_EXAMPLES).getValue();
            long noOfIncorrectPositiveExamples = counters.findCounter(JobCounters.NO_OF_INCORRECT_POSITIVE_EXAMPLES).getValue();
            long noOfIncorrectNegativeExamples = counters.findCounter(JobCounters.NO_OF_INCORRECT_NEGATIVE_EXAMPLES).getValue();
            long totalExamples = noOfPositiveExamples + noOfNegativeExamples;
            long mistakes = noOfIncorrectPositiveExamples + noOfIncorrectNegativeExamples;
            long correct = totalExamples - mistakes;
            double accuracy = (double)correct/totalExamples;
            System.out.println("Positive Ex"+noOfPositiveExamples);
            System.out.println("Negative Ex"+noOfNegativeExamples);
            System.out.println("Incorrect Pos"+noOfIncorrectPositiveExamples);
            System.out.println("Incorrect Neg"+noOfIncorrectNegativeExamples);
            System.out.println("Accuracy"+accuracy);
         }
         
      }
      catch (Exception e)
      {
         System.out.print("Unable to predict");
         e.printStackTrace();
      }
      
      
      /*List<String> predictions = new ArrayList<String>();
      try {
         Reader reader = new FileReader(args[1]);
         BufferedReader bufferedReader = new BufferedReader(reader);
         FileWriter writer = new FileWriter("labels.txt");
         String line;
         String[] split;
         String[] featureWeightSplit;
         Map<Integer, Double> featureVector;
         double sigmoidValue;
         int splitLen;
         int mistakes = 0;
         int testingDataSize = 0;
         String predictedLabel;
         while((line = bufferedReader.readLine()) != null) {
            featureVector = new HashMap<Integer, Double>();
            split = line.split(" ");
            splitLen = split.length;
            writer.write(split[0]+"\n");
            testingDataSize++;
            for(int i = 1; i<splitLen; i++) {
               featureWeightSplit = split[i].split(":");
               featureVector.put(Integer.parseInt(featureWeightSplit[0]), Double.parseDouble(featureWeightSplit[1]));
            }
            sigmoidValue = getSigmoidFunctionValue(featureVector);
            System.out.println(sigmoidValue);
            if(sigmoidValue >=0.5) {
               predictedLabel = "+1";
            } else {
               predictedLabel = "-1";
            }
            if(!split[0].equals(predictedLabel)) {
               mistakes++;
            }
            predictions.add(predictedLabel);
         }
         System.out.println("No Of Mistakes"+mistakes);
         double accuracy = (double)(testingDataSize - mistakes)/testingDataSize;
         System.out.println("Accuracy:"+accuracy);
         bufferedReader.close();
         writer.close();
      } catch (Exception e) {
         System.out.println("Cant read testing data!");
         System.exit(1);
      }
      return predictions;*/
   }
   
   public static void main(String[] args) {
      train(args);
      predict(args);
   }
}