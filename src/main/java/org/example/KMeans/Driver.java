package org.example.KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.LineReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Driver {

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, VectorPair> {

        private final ArrayList<Vector> centers = new ArrayList<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path("/lab423/tmp/centers.data");//读取20个中心点数据
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {//按行处理中心点，共有20行，每行是一个ID+16位向量
                StringTokenizer itr = new StringTokenizer(line);
                itr.nextToken();//跳过ID
                Vector center = new Vector();//本行中心点对应的向量
                for (int i = 0; i < 16; i++) {//读取数据，设置向量16位的值
                    String v = itr.nextToken();
                    if (v.charAt(v.length() - 1) == ',') {//跳过逗号
                        v = v.substring(0, v.length() - 1);
                    }
                    center.set(i, Double.parseDouble(v));//设置第i位
                }
                centers.add(center);//将该向量加入到中心点集合中
            }
            reader.close();
            inputStream.close();
            //fs.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());//将输入数据序列化
            String no = itr.nextToken();//跳过no
            Vector vec = new Vector();
            for (int i = 0; i < 16; i++) {//读取本行向量16位数值
                String v = itr.nextToken();
                if (v.charAt(v.length() - 1) == ',') {
                    v = v.substring(0, v.length() - 1);
                }
                vec.set(i, Double.parseDouble(v));
            }
            double minDis = Double.MAX_VALUE;
            int idx = -1;
            for (int i = 0; i < centers.size(); i++) {//找出距离最小的中心点
                double dis = vec.computeDist(centers.get(i));
                if (dis < minDis) {
                    minDis = dis;
                    idx = i;
                }
            }
            context.write(new IntWritable(idx), new VectorPair(vec, 1));//输出中心点ID + 本向量
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, VectorPair, Text, Text> {
        private MultipleOutputs<Text, Text> multiOs;

        @Override
        protected void setup(Context context){
            multiOs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(IntWritable key, Iterable<VectorPair> values, Context context) throws IOException,
                InterruptedException {
            Vector avg = new Vector();
            int n = 0;
            for (VectorPair vectorPair: values) {
                //选作内容：将本向量分配到对应的簇中
                multiOs.write(new Text(key.toString()), new Text(vectorPair.getVector().toString()),
                        "cluster_" + new Text(key.toString()));
                //累加本簇中所有的向量，准备求平均
                avg.add(vectorPair.getVector());
                n += vectorPair.getCount();
            }
            for (int i = 0; i < 16; i++) {
                //计算新的中心点向量，保留6位小数。
                double result = (double) Math.round((avg.get(i) / n) * 1000000) /  1000000;
                avg.set(i, result);
            }
            //输出中心点ID + 新的中心点向量
            context.write(new Text(key.toString()), new Text(avg.toString()));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            multiOs.close();
        }
    }
    public static void deletePath(String pathStr, boolean isDeleteDir) throws IOException {
        if(isDeleteDir) pathStr = pathStr.substring(0, pathStr.lastIndexOf('/'));
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }

    public static void copyFile(String from_path, String to_path) throws IOException {
        Path path_from = new Path(from_path);
        Path path_to = new Path(to_path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path_from.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);
        Text line = new Text();
        while(lineReader.readLine(line) > 0) {
            String str = line.toString() + "\n";
            outputStream.write(str.getBytes());
        }
        lineReader.close();
        outputStream.close();
    }


    private static boolean compare(String new_Path) throws IOException {
        //TODO
        Path oldPath = new Path("/lab423/tmp/centers.data");
        Path newPath = new Path(new_Path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = newPath.getFileSystem(configuration);
        FSDataInputStream newStream = fileSystem.open(newPath);
        FSDataInputStream oldStream = fileSystem.open(oldPath);
        LineReader newlineReader = new LineReader(newStream, configuration);
        LineReader oldlineReader = new LineReader(oldStream, configuration);
        Text line1 = new Text();
        Text line2 = new Text();
        while(newlineReader.readLine(line1) > 0 && oldlineReader.readLine(line2) > 0){
            if(!line2.equals(line1)){
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Path tmpDir = new Path("/lab423/tmp");
            FileSystem fileSystem = tmpDir.getFileSystem(conf);
            fileSystem.mkdirs(tmpDir);
            copyFile(args[0] + "/initial_centers.data", "/lab423/tmp/centers.data");
            int times = 0;
            while (true) {
                Job job = Job.getInstance(conf, "KMeans");
                job.setJarByClass(Driver.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(Driver.KMeansMapper.class);
                job.setReducerClass(Driver.KMeansReducer.class);
                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(VectorPair.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                // TODO: change file name
                FileInputFormat.addInputPath(job, new Path(args[0] + "/dataset_mini.data"));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.waitForCompletion(true);
                if (!compare(args[1]+ "/part-r-00000")) {
                    break;
                }
                copyFile(args[1]+ "/part-r-00000", "/lab423/tmp/centers.data");
                deletePath(args[1]+ "/part-r-00000",true);
                times ++;
                System.out.println(times);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}