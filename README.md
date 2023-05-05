# Spacker

Spacker is a unified framework that enables configurable state migration to achieve varying performance trade-offs.

Spacker stores its abstraction at `StateMigrationPlanner.java`, in which Spacker also encapsulates APIs for flexible definition of `order function`,  `batch function`, and `replication function`.

## Prerequisite

1. Python3
2. Zookeeper
3. Kafka
4. Java 1.8

## Code architecture

The source code of `Spacker` has been placed into `Flink`, because Flink has network stack for us to achieve RPC among our components.

The main entrypoint of Spacker can be found in `flink-runtime`.

To explore our source code, you can try to start from `JobStateCoordinator.java`, which is the main component to connect other components and  to execute state migration in fine-grained.

- Spacker-on-Flink: The source code of Spacker.
- flink-testbed: The workloads and experiment scripts used for evaluation.

## How to use?

### Run an example

1. Compile `Spacker-on-Flink` with : `mvn clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true`.
2. Compile `flink-test` with: `mvn clean package`.
3. Try Spacker with the following command: `cd Spacker-on-Flink/build-target`  and start a standalone cluster: `./bin/start-cluster.sh`.
4. Launch an example `MicroBenchmark`  in  examples folder: `./bin/flink run -c flinkapp.MicroBenchmark examples/target/testbed-1.0-SNAPSHOT.jar`
5. We have implemented a default planning strategy, which exposes configurations via `flink-conf.yaml`, you may set varying thresholds for each tuning options in Spacker. The followings are some configurations you can try in `flink-conf.yaml`:

| Parameter                   | Default          | Description                                                  |
| --------------------------- | ---------------- | ------------------------------------------------------------ |
| spector.reconfig.scenario   | shuffle          | The experiment scenarios that are aligned with our paper. The default one is to try state migration. You can also configure `profiling` and `dynamic` for different experiments. |
| controller.target.operators | Splitter FlatMap | To control the operators to apply a reconfiguration for state migration. |
| spector.reconfig.start      | 10000            | To control when to start a reconfiguration                   |
| trisk.exp.dir               | /data            | the folder to output metrics measured during the execution.  |

## Run scripts for experiments

In this project, we have mainly run experiments on three systems as illustrated in our paper:

1. Performance Overview experiment
2. System Overhead experiment
3. Workload Characteristics experiment

We have placed our scripts to run the experiments in `flink-testbed/exp_scripts` folder, in which there mainly four sub-folders:

- `analysis`: Figures drawing scripts for all experiments.
- `flink-conf`: configurations for Flink cluster.
- `spector_reconfig`: Scripts to run all experiments.

All experiments exposed their configurations in:

- `analysis/config/default_config.py`
- `spector_reconfig/config.sh`

For scripts to draw figures:

| Variable      | Default         | Description            |
| ------------- | --------------- | ---------------------- |
| FIGURE_FOLDER | '/data/results' | path to output figures |
| FILE_FOLER    | '/data/raw'     | path to read raw data  |

