package es.upm.cloud.flink.corregidos;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author Ainhoa Azqueta Alzúaz (aazqueta@fi.upm.es)
 * @organization Universidad Politécnica de Madrid
 * @laboratory Laboratorio de Sistemas Distributidos (LSD)
 * @date 18/11/23
 **/
public class Exercise4 {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> text;
        text = env.readTextFile("./files/sensorData.csv");

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapStream = text.
                map(new MapFunction<String, Tuple3<Long, String, Double>>() {
                    public Tuple3<Long, String, Double> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple3<Long,String,Double> out = new Tuple3(Long.parseLong(fieldArray[0]),
                                fieldArray[1], Double.parseDouble(fieldArray[2]));
                        return out;
                    }
                });
        KeyedStream<Tuple3<Long, String, Double>, String> keyedStream = mapStream.keyBy(new KeySelector<Tuple3<Long, String, Double>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, Double> longStringDoubleTuple3) throws Exception {
                return longStringDoubleTuple3.f1;
            }
        });

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> output = keyedStream.reduce(new ReduceFunction<Tuple3<Long, String, Double>>() {
            @Override
            public Tuple3<Long, String, Double> reduce(Tuple3<Long, String, Double> value1, Tuple3<Long, String, Double> value2) throws Exception {
                return new Tuple3<Long, String, Double>(value2.f0, value1.f1,value1.f2+value2.f2);
            }
        });

        output.writeAsCsv("./files/ex4.csv", FileSystem.WriteMode.OVERWRITE);
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        text.print();
        env.execute("Exercise4");
    }

}
