package lab;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Runs the pipelines and writes docs/data.json for the Pages viewer. */
public final class Report {
  static final long MIN = 60_000L;

  static List<Event> sample() {
    Random r = new Random(7);
    List<Event> es = new ArrayList<>();
    String[] users = {"ana", "bob", "cy", "di"};
    for (int i = 0; i < 400; i++) es.add(new Event("e" + i, users[r.nextInt(4)], r.nextInt(10 * 60) * 1000L, 1 + r.nextInt(50)));
    for (int i = 0; i < 20; i++) es.add(es.get(r.nextInt(400)));
    return es;
  }

  static List<String> read(Path dir) throws Exception {
    List<String> out = new ArrayList<>();
    try (var s = Files.list(dir)) {
      for (Path f : s.filter(x -> x.getFileName().toString().startsWith("out")).collect(Collectors.toList())) out.addAll(Files.readAllLines(f));
    }
    out.sort(null);
    return out;
  }

  static final List<String> COLLECTED = java.util.Collections.synchronizedList(new ArrayList<>());

  /** In-process sink; fine because the DirectRunner runs in this JVM. */
  static final class Collect extends DoFn<String, Void> {
    @ProcessElement public void process(@Element String s) { COLLECTED.add(s); }
  }

  static String q(String s) { return "\"" + s.replace("\"", "\\\"") + "\""; }
  static String arr(List<String> l) { return "[" + l.stream().map(Report::q).collect(Collectors.joining(",")) + "]"; }

  public static void main(String[] args) throws Exception {
    List<Event> es = sample();

    long t0 = System.nanoTime();
    Path d1 = Files.createTempDirectory("direct");
    Pipeline p1 = Pipeline.create(PipelineOptionsFactory.create());
    Pipelines.batchTotals(p1.apply(Create.of(es).withCoder(SerializableCoder.of(Event.class)))).apply(TextIO.write().to(d1.resolve("out").toString()).withSuffix(".txt"));
    p1.run().waitUntilFinish();
    long directMs = (System.nanoTime() - t0) / 1_000_000;
    List<String> direct = read(d1);

    FlinkPipelineOptions o = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    o.setRunner(FlinkRunner.class);
    o.setFlinkMaster("[local]");
    o.setParallelism(2);
    t0 = System.nanoTime();
    Path d2 = Files.createTempDirectory("flink");
    Pipeline p2 = Pipeline.create(o);
    Pipelines.batchTotals(p2.apply(Create.of(es).withCoder(SerializableCoder.of(Event.class)))).apply(TextIO.write().to(d2.resolve("out").toString()).withSuffix(".txt"));
    p2.run().waitUntilFinish();
    long flinkMs = (System.nanoTime() - t0) / 1_000_000;
    List<String> flink = read(d2);
    List<String> directNoPane = direct.stream().map(l -> l.substring(0, l.lastIndexOf('|'))).collect(Collectors.toList());
    List<String> flinkNoPane = flink.stream().map(l -> l.substring(0, l.lastIndexOf('|'))).collect(Collectors.toList());

    // Streaming scenario on TestStream: on-time pane, late pane within lateness, event beyond lateness dropped.
    TestStream<Event> stream = TestStream.create(SerializableCoder.of(Event.class))
        .addElements(TimestampedValue.of(new Event("1", "ana", 5_000, 10), new Instant(5_000)),
                     TimestampedValue.of(new Event("2", "ana", 50_000, 5), new Instant(50_000)))
        .advanceWatermarkTo(new Instant(MIN + 1))
        .advanceProcessingTime(Duration.standardSeconds(1))
        .addElements(TimestampedValue.of(new Event("3", "ana", 55_000, 4), new Instant(55_000)))
        .advanceWatermarkTo(new Instant(4 * MIN))
        .addElements(TimestampedValue.of(new Event("4", "ana", 58_000, 100), new Instant(58_000)))
        .advanceWatermarkToInfinity();
    Pipeline p3 = Pipeline.create(PipelineOptionsFactory.create());
    PCollection<String> s = Pipelines.streamingTotals(p3.apply(stream), false);
    s.apply(ParDo.of(new Collect()));
    p3.run().waitUntilFinish();
    List<String> panes = new ArrayList<>(COLLECTED);
    panes.sort(null);

    String json = "{\n"
        + " \"events\": " + es.size() + ",\n"
        + " \"distinctIds\": " + es.stream().map(e -> e.id).distinct().count() + ",\n"
        + " \"direct\": {\"ms\": " + directMs + ", \"rows\": " + arr(direct) + "},\n"
        + " \"flink\": {\"ms\": " + flinkMs + ", \"rows\": " + arr(flink) + "},\n"
        + " \"identical\": " + directNoPane.equals(flinkNoPane) + ",\n"
        + " \"streaming\": {\"input\": " + arr(List.of(
            "t=5s ana +10 (id 1)", "t=50s ana +5 (id 2)", "watermark passes the end of minute 0",
            "t=55s ana +4 (id 3) arrives late, inside the 2 minute allowed lateness",
            "watermark reaches 4 minutes: minute 0 is closed",
            "t=58s ana +100 (id 4) arrives after the window closed")) + ", \"panes\": " + arr(panes) + "}\n"
        + "}\n";
    Files.writeString(Path.of("docs/data.json"), json);
    System.out.println("direct " + directMs + "ms rows=" + direct.size() + " | flink " + flinkMs + "ms rows=" + flink.size() + " | identical=" + directNoPane.equals(flinkNoPane));
    System.out.println("panes: " + panes);
    System.exit(0);
  }
}
