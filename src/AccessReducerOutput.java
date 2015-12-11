import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class AccessReducerOutput
{
   private static String bucketName = "com.mapreduce.homework1"; 
   private static String key        = "LiveRamp.txt";
   
   public static String readFile(String filePath) {
      BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAISTZGGADJPGMNY2Q", "mPZV4DQEkYM+RLwXJQhcqk8vBIj3SzF4EWCRbfMe");
      AmazonS3Client s3Client = new AmazonS3Client(awsCreds);
      String line = null;
      try {
          System.out.println("Downloading an object");
          S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, filePath));
          System.out.println("Content-Type: "  + s3object.getObjectMetadata().getContentType());
          BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
          line = reader.readLine();
      } catch (AmazonServiceException ase) {

          System.out.println("Caught an AmazonServiceException, which" + " means your request made it " + "to Amazon S3, but was rejected with an error response" + " for some reason.");
          System.out.println("Error Message:    " + ase.getMessage());
          System.out.println("HTTP Status Code: " + ase.getStatusCode());
          System.out.println("AWS Error Code:   " + ase.getErrorCode());
          System.out.println("Error Type:       " + ase.getErrorType());
          System.out.println("Request ID:       " + ase.getRequestId());

      } catch (AmazonClientException ace) {
          System.out.println("Caught an AmazonClientException, which means"+" the client encountered " + "an internal error while trying to " +"communicate with S3, " +"such as not being able to access the network.");
          System.out.println("Error Message: " + ace.getMessage());
      } catch (Exception e) {
         e.printStackTrace();
      }
      return line;
   }
}
