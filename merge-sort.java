import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Arrays;

public class MergeSort {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            word.set(line);
            context.write(word, new Text());
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, new Text());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "merge sort");
        job.setJarByClass(MergeSort.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

import java.io.*;
import java.util.concurrent.*;

public class MultithreadedMergeSort {

    public static void main(String[] args) {
        String inputFile = "input.txt";
        String outputFile = "output.txt";
        int numberOfThreads = 4; // Number of threads to use

        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            // Read lines from input file
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            reader.close();

            // Split lines into array
            String[] lines = sb.toString().split("\n");

            // Multithreaded merge sort
            ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
            Future<String[]> future = executor.submit(new MergeSortTask(lines));
            String[] sortedLines = future.get(); // Get the sorted array
            executor.shutdown();

            // Write sorted lines to output file
            for (String sortedLine : sortedLines) {
                writer.write(sortedLine);
                writer.newLine();
            }
            writer.close();

            System.out.println("Merge sort completed successfully.");

        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    static class MergeSortTask implements Callable<String[]> {
        private final String[] array;

        MergeSortTask(String[] array) {
            this.array = array;
        }

        @Override
        public String[] call() {
            mergeSort(array, 0, array.length - 1);
            return array;
        }

        private void mergeSort(String[] array, int left, int right) {
            if (left < right) {
                int mid = left + (right - left) / 2;
                mergeSort(array, left, mid);
                mergeSort(array, mid + 1, right);
                merge(array, left, mid, right);
            }
        }

        private void merge(String[] array, int left, int mid, int right) {
            int n1 = mid - left + 1;
            int n2 = right - mid;

            String[] L = new String[n1];
            String[] R = new String[n2];

            System.arraycopy(array, left, L, 0, n1);
            System.arraycopy(array, mid + 1, R, 0, n2);

            int i = 0, j = 0, k = left;

            while (i < n1 && j < n2) {
                if (L[i].compareTo(R[j]) <= 0) {
                    array[k] = L[i];
                    i++;
                } else {
                    array[k] = R[j];
                    j++;
                }
                k++;
            }

            while (i < n1) {
                array[k] = L[i];
                i++;
                k++;
            }

            while (j < n2) {
                array[k] = R[j];
                j++;
                k++;
            }
        }
    }
}
