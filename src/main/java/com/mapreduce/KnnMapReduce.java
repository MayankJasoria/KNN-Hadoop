package com.mapreduce;

import com.Globals;
import com.utils.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Time;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class KnnMapReduce {

    public static long execute() throws IOException, ClassNotFoundException, InterruptedException {
        // setup Map Reduce configurations
        Configuration conf = new Configuration();

        // like defined in hdfs-site.xml (required for reading file from hdfs)
        conf.set("fs.defaultFS", Globals.getNamenodeUrl());
        conf.setInt("kvalue", Globals.getKvalue());
        conf.set("trainFile", Globals.getFileInputPath() + Globals.getTrainFileName());

        // TODO: add other configuration as required

        Job job = Job.getInstance(conf, "KnnMR");
        job.setJarByClass(KnnMapReduce.class);

        // add Mapper class
        job.setMapperClass(KnnMapper.class);
        // passing the required csv file as file path
        MultipleInputs.addInputPath(job,
                new Path(Globals.getFileInputPath() + Globals.getTestFileName()),
                TextInputFormat.class, KnnMapper.class);

//        MultipleInputs.addInputPath(job,
//                new Path(Globals.getFileInputPath() + Globals.getTrainFileName()),
//                TextInputFormat.class, KnnMapper.class);

        // add combiner class
        job.setCombinerClass(KnnCombiner.class);

        // add reducer class
        job.setReducerClass(KnnReducer.class);

        // define key and value output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // execute
        Path outputPath = new Path(Globals.getHadoopOutputPath());
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        long startTime = Time.now();
        long endTime = (job.waitForCompletion(true) ? Time.now() : startTime); // makes execution a blocking/nonblocking operation

        return endTime - startTime;
    }

    private static class KnnMapper extends Mapper<Object, Text, Text, Text> {

        // define member variables here
        private Path trainFile;
        private FileSystem hdfs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // initialize member variables here, from context.getConfiguration() if needed
            trainFile = new Path(context.getConfiguration().get("trainFile"));
            hdfs = FileSystem.get(context.getConfiguration());
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Your Map Logic Here
            String[] values = value.toString().split(" ");
            Float[] testFeatures = new Float[values.length - 1];
            for (int i = 0; i < testFeatures.length; i++) {
                testFeatures[i] = Float.parseFloat(values[i]);
            }

            // open the training file
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(trainFile)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] elements = line.split(" ");
                Float[] features = new Float[elements.length - 1];

                for (int i = 0; i < features.length; i++) {
                    features[i] = Float.parseFloat(elements[i]);
                }

                float dist = euclideanDist(testFeatures, features);
                String classLabel = elements[elements.length - 1];
                context.write(value, new Text(dist + "," + classLabel));
            }
            br.close();
        }

        private float euclideanDist(Float[] feature, Float[] test) {
            float distance = 0;
            for (int j = 0; j < test.length; j++) {
                distance += Math.pow((feature[j] - test[j]), 2);
            }
            distance = (float) Math.sqrt(distance);
            return distance;
        }
    }

    private static class KnnCombiner extends Reducer<Text, Text, Text, Text> {

        private int k;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("kvalue", 5);
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Entry> list = new ArrayList<>();
            for (Text value : values) {
                String[] val = value.toString().split(",");
                list.add(new Entry(val[0], val[1]));
            }

            list.sort(new Comparator<Entry>() {
                @Override
                public int compare(Entry e1, Entry e2) {
                    return e1.getKey().compareTo(e2.getKey());
                }
            });

            for (int i = 0; i < k; i++) {
                context.write(key, new Text(list.get(i).getKey() + "," + list.get(i).getValue()));
            }
        }
    }

    private static class KnnReducer extends Reducer<Text, Text, Text, Text> {

        // define member variables here
        private int k;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("kvalue", 5);
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Entry> list = new ArrayList<>();
            for (Text value : values) {
                String[] val = value.toString().split(",");
                list.add(new Entry(val[0], val[1]));
            }

            list.sort(new Comparator<Entry>() {
                @Override
                public int compare(Entry e1, Entry e2) {
                    return e1.getKey().compareTo(e2.getKey());
                }
            });

            HashMap<String, Integer> map = new HashMap<>();
            for (int i = 0; i < k; i++) {
                if (map.containsKey(list.get(i).getValue())) {
                    map.put(list.get(i).getValue(), map.get(list.get(i).getValue()) + 1);
                } else {
                    map.put(list.get(i).getValue(), 1);
                }
            }

            int largest = 0;
            String label = null;

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                if (Math.max(largest, entry.getValue()) != largest) {
                    largest = entry.getValue();
                    label = entry.getKey();
                }
            }

            context.write(key, new Text(label));
        }
    }
}
