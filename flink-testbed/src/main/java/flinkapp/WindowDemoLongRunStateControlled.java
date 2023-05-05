package flinkapp;

import Nexmark.sources.Util;
import common.MathUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static common.Util.delay;

/**
 * Test Job for the most fundemental functionalities,
 * 1. single scaling, mutliple scaling.
 * 2. different arrival rates, testing scaling on under-loaded job and over-loaded job.
 * 3. different key distributions, whether the final key count is consistent.
 */

public class WindowDemoLongRunStateControlled {

    private static final int MAX = 1000000 * 10;
    //    private static final int MAX = 1000;
    private static final int NUM_LETTERS = 26;

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(params.getInt("interval", 1000));
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // set up the execution environment
        env.setStateBackend(new MemoryStateBackend(1073741824));

        int perKeyStateSize = params.getInt("perKeySize", 1024);
        int stateAccessRatio = params.getInt("stateAccessRatio", 25); // default 25%

        DataStreamSource<Tuple2<String, String>> source = env.addSource(new MySource(
                params.getInt("runtime", 10),
                params.getInt("nTuples", 10000),
                params.getInt("nKeys", 1000),
                params.getInt("mp2", 128),
                stateAccessRatio
        )).setParallelism(params.getInt("p1", 1));
        DataStream<Long> windowed = source
                .slotSharingGroup("g1")
                .keyBy(0)
//                .map(new MyStatefulMap(perKeyStateSize))
                .timeWindow(Time.seconds(params.getInt("window-size", 2)), Time.seconds(1))
                .aggregate(new MyAggregateFun());

        ((SingleOutputStreamOperator<Long>) windowed).disableChaining();
        ((SingleOutputStreamOperator<Long>) windowed).setMaxParallelism(params.getInt("mp2", 64));
        ((SingleOutputStreamOperator<Long>) windowed).setParallelism(params.getInt("p2",  1));
        ((SingleOutputStreamOperator<Long>) windowed).name("window");

        env.execute();
    }

    private static final class MyAggregateFun implements AggregateFunction<Tuple2<String, String>, Long, Long> {

        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, String> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }


    private static class MyStatefulMap extends RichMapFunction<Tuple2<String, String>, String> {

        private transient MapState<String, String> countMap;

        private int count = 0;

        private final int perKeyStateSize;

        private final String payload;

        public MyStatefulMap(int perKeyStateSize) {
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
        }

        @Override
        public String map(Tuple2<String, String> input) throws Exception {
            delay(100_000);

            String s = input.f0;

            Long cur = 1L;
            countMap.put(s, payload);

//            Long cur = countMap.get(s);
//            cur = (cur == null) ? 1 : cur + 1;
//            countMap.put(s, cur);

//            count++;
//            System.out.println("counted: " + s + " : " + cur);

//            System.out.println("ts: " + Long.parseLong(input.f1) + " endToEnd latency: " + (System.currentTimeMillis() - Long.parseLong(input.f1)));

            return String.format("%s %d", s, cur);
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>("word-count", String.class, String.class);

            countMap = getRuntimeContext().getMapState(descriptor);
        }
    }

    private static class MySource extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {

        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        private int runtime;
        private final int nTuples;
        private final int nKeys;
        private final int rate;

        private final int maxParallelism;

        private final int subKeyGroupSize;

        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        MySource(int runtime, int nTuples, int nKeys, int maxParallelism, int stateAccessRatio) {
            this.runtime = runtime;
            this.nTuples = nTuples;
            this.nKeys = nKeys;
            this.rate = nTuples / runtime;
            this.maxParallelism = maxParallelism;
            this.subKeyGroupSize = maxParallelism * stateAccessRatio / 100;

            // Another functionality test
            for (int i = 0; i < nKeys; i++) {
                String key = "A" + i;
                int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
                List<String> keys = keyGroupMapping.computeIfAbsent(keygroup, t -> new ArrayList<>());
                keys.add(key);
            }

            System.out.println("runtime: " + runtime
                    + ", nTuples: " + nTuples
                    + ", nKeys: " + nKeys
                    + ", rate: " + rate);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            this.checkpointedCount.clear();
            this.checkpointedCount.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.checkpointedCount = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("checkpointedCount", Integer.class));

            if (context.isRestored()) {
                for (Integer count : this.checkpointedCount.get()) {
                    this.count = count;
                }
            }
        }

        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

            List<String> subKeySet = Util.selectKeys(subKeyGroupSize, keyGroupMapping);
            Map<Integer, Integer> keyGroupCount = new HashMap<>();


            long emitStartTime = System.currentTimeMillis();

            while (isRunning && count < nTuples) {

                emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < rate / 20; i++) {
                    String key = getSubKeySetChar(count, subKeySet);

                    int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
//                    int curCount = keyGroupCount.getOrDefault(keygroup, 0) + 1;
//                    keyGroupCount.put(keygroup, curCount);
//                    System.out.println("sent: " + key + " : " + curCount + " total: " + count);
//                    ctx.collect(Tuple2.of(key, key));
                    ctx.collect(Tuple2.of(key, String.valueOf(System.currentTimeMillis())));

                    count++;
                }

                if (count % rate == 0) {
                    // update the keyset
//                    System.out.println("++++++Actual Keygroups accessed: " + keyGroupCount.size());
//                    keyGroupCount.clear();
//                    System.out.println("++++++new Key Set: " + subKeySet.size());
                    subKeySet = Util.selectKeys(subKeyGroupSize, keyGroupMapping);
                }

                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
            }
        }

        private String getChar(int cur) {
            return "A" + (cur % nKeys);
        }
        private String getSubKeySetChar(int cur, List<String> subKeySet) {
            return subKeySet.get(cur % subKeySet.size());
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
