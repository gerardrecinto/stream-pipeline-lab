# stream-pipeline-lab

One Apache Beam pipeline, run as a batch job on two runners (the Beam DirectRunner and a local Apache Flink) and as a streaming job with late data on a test stream.

**Live demo:** https://gerardrecinto.github.io/stream-pipeline-lab/ shows the recorded output: identical per-user, per-minute totals from both runners, and the on-time and late panes for a window that receives late data.

Personal learning project. Local single-JVM runs only: no cluster, no load test, no production use.

## What is in it

`Pipelines.java` defines the logic once:

- stamp each event with its own event time
- fixed 1-minute event-time windows
- drop repeated event ids with per-id state inside the window
- sum amount per user per window

`batchTotals` uses plain windows. `streamingTotals` adds a trigger that fires when the watermark passes the end of the window and again for each late element, with 2 minutes of allowed lateness and accumulating panes.

## Tests (`mvn test`)

- batch sums per user and minute, with a duplicate delivery dropped
- streaming: an on-time pane (sum 15), then a late pane (sum 19) for an element that arrives after the watermark but within allowed lateness, and an element that arrives after the lateness is never emitted
- the final streaming answer equals a batch answer computed by hand, for 60 events delivered in shuffled order, with duplicates, and half of them held back until the watermark has passed the first two windows
- the same batch pipeline on the DirectRunner and on a local Flink runner (`--flinkMaster=[local]`, parallelism 2) gives identical results for 420 input events

## Things I ran into

- **Beam's `Distinct` broke event-time windowing.** Applied before windowing it groups in the global window and re-stamps each element at the end of time, so all events fell into one window (my output showed a window start near the year 292 billion). Applied after a triggered window it would re-emit accumulated elements on every late pane. I replaced it with a stateful step keyed by event id inside the window.
- **Pane timing differs by runner.** Flink in batch mode reported pane timing UNKNOWN where the DirectRunner said ON_TIME. The cross-runner test compares the data and ignores the pane label.
- **The stateful dedupe step changed the on-time and late split on the DirectRunner.** With it in front of the trigger, the late element was folded into the on-time pane. Without it the panes split as expected. So the pane test runs without dedupe, and dedupe is tested in the batch and shuffled-arrival tests. I did not work out whether that is documented runner behavior or a flaw in my test.
- **My first shuffled-arrival test was wrong.** I held events back until the watermark passed 4 minutes, which is past the allowed lateness of the early windows, so Beam correctly dropped them and my expected totals did not match. The fix was in the test, not the pipeline.
- Timings in `docs/data.json` are not a benchmark. The first run in a JVM pays class loading and the DirectRunner ran first.

## Not covered

- Apache Spark (I did not run the Spark runner)
- Apache Kafka as a source or sink; see my `kafka-ops-lab` repo for Kafka operations
- a real cluster, checkpointing and recovery, exactly-once sinks, and any throughput measurement

## Run it

```
mvn test
mvn -q dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt && mvn -q compile
java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -cp target/classes:$(cat /tmp/cp.txt) lab.Report
python3 -m http.server -d docs
```

(`pom.xml` carries the full list of `--add-opens` flags that Flink needs on a recent JDK.)

## References

- Apache Beam: [Programming Guide](https://beam.apache.org/documentation/programming-guide/), [runner capability matrix](https://beam.apache.org/documentation/runners/capability-matrix/), [Flink runner](https://beam.apache.org/documentation/runners/flink/), [Stateful processing](https://beam.apache.org/blog/stateful-processing/)
- Apache Flink: [documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- Apache Spark: [streaming programming guide](https://spark.apache.org/docs/latest/streaming/)
- Apache Kafka: [documentation](https://kafka.apache.org/documentation/)
- Akidau et al., [The Dataflow Model](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf) (VLDB 2015); Akidau, [The world beyond batch: Streaming 101](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/)
