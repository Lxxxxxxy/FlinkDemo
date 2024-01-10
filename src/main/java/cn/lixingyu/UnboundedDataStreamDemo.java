package cn.lixingyu;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Lxxxxxxy_
 * @date 2024/01/03 10:19
 */
public class UnboundedDataStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("localhost", 9000);
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns =
            dataStreamSource.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                for (String word : line.split(" ")) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }).returns(Types.TUPLE(Types.STRING, Types.INT));
        returns.keyBy(data -> data.f0).sum(1).print();
        executionEnvironment.execute();
    }
}
