package es.upm.cloud.flink.corregidos;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author Ainhoa Azqueta Alzúaz (aazqueta@fi.upm.es)
 * @organization Universidad Politécnica de Madrid
 * @laboratory Laboratorio de Sistemas Distributidos (LSD)
 * @date 18/11/23
 **/
public class Exercise7 {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        KeyedStream<Tuple3<Long, String, Double>, String> keyedStream = mapStream.
                assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<Long, String, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> event.f0*1000))
                .keyBy(new KeySelector<Tuple3<Long, String, Double>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, Double> longStringDoubleTuple3) throws Exception {
                return longStringDoubleTuple3.f1;
            }
        });

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> output = keyedStream.
                window(TumblingEventTimeWindows.of(Time.seconds(3))).reduce(new MyReduceFunction());

        output.writeAsCsv("./files/sensorData7.csv", FileSystem.WriteMode.OVERWRITE);
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        text.print();

        env.execute("Exercise7");
    }

    private static class MyReduceFunction implements ReduceFunction<Tuple3<Long, String, Double>> {

        @Override
        public Tuple3<Long, String, Double> reduce(Tuple3<Long, String, Double> r1, Tuple3<Long, String, Double> r2) throws Exception {
            if(r1.f2 < r2.f2){
                return r1;
            }
            else{
                return r2;
            }
        }
    }
}
