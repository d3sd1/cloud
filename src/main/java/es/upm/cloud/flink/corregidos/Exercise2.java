package es.upm.cloud.flink.corregidos;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Ainhoa Azqueta Alzúaz (aazqueta@fi.upm.es)
 * @organization Universidad Politécnica de Madrid
 * @laboratory Laboratorio de Sistemas Distributidos (LSD)
 * @date 18/11/23
 **/
public class Exercise2 {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text;
        text = env.readTextFile("./files/sensorData.csv");

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapStream = text.map(new MapFunction<String, Tuple3<Long, String, Double>>() {
            public Tuple3<Long, String, Double> map(String in) throws Exception {
                String[] fieldArray = in.split(",");
                Tuple3<Long, String, Double> out = new Tuple3(Long.parseLong(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
                return out;
            }
        }).filter(new FilterFunction<Tuple3<Long, String, Double>>() {
            @Override
            public boolean filter(Tuple3<Long, String, Double> tuple) throws Exception {
                return tuple.f1.equals("sensor1");
            }
        });
        mapStream.writeAsCsv("./files/ex2.csv", FileSystem.WriteMode.OVERWRITE);
        if (params.has("output")) {

        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }

        env.execute("Exercise2");
    }

}
