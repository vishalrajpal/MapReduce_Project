
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable
{
   DoubleWritable[] array;
   public DoubleArrayWritable() {
      super(DoubleWritable.class);
   }
   
   public DoubleArrayWritable(DoubleWritable[] array)
   {
      super(DoubleWritable.class);
      this.array = array;
      set(array);
   }
   
   public DoubleWritable[] get() {
      Writable[] arr = super.get();
      DoubleWritable[] tempArr = new DoubleWritable[arr.length];
      for(int i = 0; i<arr.length; i++) {
         tempArr[i] = (DoubleWritable)arr[i];
      }
      return tempArr;
   }

   @Override
   public String[] toStrings()
   {
      int arrayLen = array.length;
      String[] strings = new String[arrayLen];
      for(int i = 0; i<arrayLen; i++) {
         strings[i] = array[i].toString();
      }
      return strings;
   }

   @Override
   public String toString()
   {
      StringBuilder res = new StringBuilder();
      Writable[] arr = super.get();
      
      for(int i = 0; i<arr.length; i++) {
         res.append(arr[i]+" ");
      }
      return res.toString();
   }
   
   
}
