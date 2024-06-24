package es.upm.cloud.flink.corregidos;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author Ainhoa Azqueta Alzúaz (aazqueta@fi.upm.es)
 * @organization Universidad Politécnica de Madrid
 * @laboratory Laboratorio de Sistemas Distributidos (LSD)
 * @date 18/11/23
 **/
public class Exercise3 {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text;
        text = env.readTextFile("./files/sensorData.csv");

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapStream = text.
                flatMap(new FlatMapFunction<String, Tuple3<Long, String, Double>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple3<Long, String, Double>> collector) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple3<Long,String,Double> out1 = new Tuple3(Long.parseLong(fieldArray[0]),
                                fieldArray[1], Double.parseDouble(fieldArray[2]));
                        collector.collect(out1);
                        Tuple3<Long,String,Double> out2 = new Tuple3(Long.parseLong(fieldArray[0]),
                                fieldArray[1]+"-F", (Double.parseDouble(fieldArray[2]) * 9 / 5) + 32);
                        collector.collect(out2);
                    }
                });

        mapStream.writeAsCsv("./files/ex3.csv", FileSystem.WriteMode.OVERWRITE);
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        text.print();
        env.execute("Exercise3");
    }

}
