package lab;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class PipelineTest {
  @Rule public final transient TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  static final long MIN = 60_000L;

  static Event ev(String id, String user, long minute, long secOffset, long amount) {
    return new Event(id, user, minute * MIN + secOffset * 1000, amount);
  }

  static PCollection<Event> create(Pipeline pipeline, List<Event> es) {
    return pipeline.apply(Create.of(es).withCoder(SerializableCoder.of(Event.class)));
  }

  @Test
  public void batchSumsPerUserPerMinuteAndDropsDuplicateIds() {
    List<Event> es = Arrays.asList(
        ev("1", "ana", 0, 5, 10), ev("2", "ana", 0, 50, 5), ev("3", "ana", 1, 10, 7),
        ev("4", "bob", 0, 20, 3), ev("2", "ana", 0, 50, 5) /* duplicate delivery of id 2 */);
    PAssert.that(Pipelines.batchTotals(create(p, es)))
        .containsInAnyOrder("ana|m0|15|ON_TIME", "ana|m1|7|ON_TIME", "bob|m0|3|ON_TIME");
    p.run().waitUntilFinish();
  }

  @Test
  public void streamingEmitsOnTimePaneThenLatePaneThenDropsTooLate() {
    // Window m0 = [0, 60s). Allowed lateness is 2 minutes after the window ends.
    TestStream<Event> stream = TestStream.create(SerializableCoder.of(Event.class))
        .addElements(TimestampedValue.of(ev("1", "ana", 0, 5, 10), new Instant(5_000)),
                     TimestampedValue.of(ev("2", "ana", 0, 50, 5), new Instant(50_000)))
        .advanceWatermarkTo(new Instant(1 * MIN + 1))            // window ends: ON_TIME pane = 15
        .advanceProcessingTime(Duration.standardSeconds(1))     // lets the runner fire the on-time pane before more data arrives
        .addElements(TimestampedValue.of(ev("3", "ana", 0, 55, 4), new Instant(55_000)))  // late, within lateness
        .advanceWatermarkTo(new Instant(4 * MIN))                // lateness over: window is closed for good
        .addElements(TimestampedValue.of(ev("4", "ana", 0, 58, 100), new Instant(58_000))) // too late: dropped
        .advanceWatermarkToInfinity();
    // dedupe off: with the stateful dedupe step in front, the DirectRunner folded the late element into
    // the on-time pane (observed, see README), which tests the runner's bundling and not the trigger.
    PCollection<String> out = Pipelines.streamingTotals(p.apply(stream), false);
    // accumulating panes: the late pane carries the running total 15 + 4, and the dropped event never appears.
    PAssert.that(out).containsInAnyOrder("ana|m0|15|ON_TIME", "ana|m0|19|LATE");
    p.run().waitUntilFinish();
  }

  @Test
  public void streamingFinalAnswerEqualsBatchAnswerForShuffledArrival() {
    Random r = new Random(42);
    List<Event> es = new ArrayList<>();
    String[] users = {"ana", "bob", "cy"};
    for (int i = 0; i < 60; i++) {
      es.add(new Event("e" + i, users[r.nextInt(3)], r.nextInt(4 * 60) * 1000L, 1 + r.nextInt(20)));
    }
    // Batch reference, computed by hand outside Beam.
    List<String> expected = es.stream()
        .collect(Collectors.groupingBy(e -> e.user + "|m" + (e.ts / MIN), Collectors.summingLong(e -> e.amount)))
        .entrySet().stream().map(x -> x.getKey() + "|" + x.getValue()).sorted().collect(Collectors.toList());

    // Streaming: arrival order shuffled, plus duplicate deliveries that the dedupe step must drop, and half of the events held back until the watermark has passed the first two windows (still within allowed lateness).
    List<Event> shuffled = new ArrayList<>(es);
    for (int i = 0; i < 10; i++) shuffled.add(es.get(r.nextInt(es.size())));
    Collections.shuffle(shuffled, r);
    TestStream.Builder<Event> b = TestStream.create(SerializableCoder.of(Event.class));
    int half = shuffled.size() / 2;
    for (Event e : shuffled.subList(0, half)) b = b.addElements(TimestampedValue.of(e, new Instant(e.ts)));
    b = b.advanceWatermarkTo(new Instant(2 * MIN + 30_000)); // windows m0 and m1 fire on time; every window is still inside its lateness
    for (Event e : shuffled.subList(half, shuffled.size())) b = b.addElements(TimestampedValue.of(e, new Instant(e.ts)));
    TestStream<Event> stream = b.advanceWatermarkToInfinity();

    // Keep only the last (largest) accumulated sum per user and window.
    PCollection<String> out = Pipelines.streamingTotals(p.apply(stream));
    PAssert.that(out).satisfies(rows -> {
      java.util.Map<String, Long> last = new java.util.HashMap<>();
      for (String row : rows) {
        String[] f = row.split("\\|");
        last.merge(f[0] + "|" + f[1], Long.parseLong(f[2]), Math::max);
      }
      List<String> got = last.entrySet().stream().map(x -> x.getKey() + "|" + x.getValue()).sorted().collect(Collectors.toList());
      assertEquals(expected, got);
      return null;
    });
    p.run().waitUntilFinish();
  }

  // ---- the same pipeline on two different runners ----

  static List<Event> sample() {
    Random r = new Random(7);
    List<Event> es = new ArrayList<>();
    String[] users = {"ana", "bob", "cy", "di"};
    for (int i = 0; i < 400; i++) es.add(new Event("e" + i, users[r.nextInt(4)], r.nextInt(10 * 60) * 1000L, 1 + r.nextInt(50)));
    for (int i = 0; i < 20; i++) es.add(es.get(r.nextInt(400))); // duplicate deliveries
    return es;
  }

  static List<String> runToFile(Pipeline pipeline, Path dir) throws Exception {
    Pipelines.batchTotals(create(pipeline, sample())).apply(TextIO.write().to(dir.resolve("out").toString()).withSuffix(".txt"));
    pipeline.run().waitUntilFinish();
    try (Stream<Path> files = Files.list(dir)) {
      List<String> lines = new ArrayList<>();
      for (Path f : files.filter(x -> x.getFileName().toString().startsWith("out")).collect(Collectors.toList())) lines.addAll(Files.readAllLines(f));
      // Strip the pane label: Flink's batch mode leaves pane timing UNKNOWN where the DirectRunner says ON_TIME.
      return lines.stream().map(l -> l.substring(0, l.lastIndexOf('|'))).sorted().collect(Collectors.toList());
    }
  }

  @Test
  public void directRunnerAndFlinkRunnerProduceIdenticalResults() throws Exception {
    Path d1 = Files.createTempDirectory("direct");
    List<String> direct = runToFile(Pipeline.create(PipelineOptionsFactory.create()), d1);

    FlinkPipelineOptions opts = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    opts.setRunner(FlinkRunner.class);
    opts.setFlinkMaster("[local]");
    opts.setParallelism(2);
    Path d2 = Files.createTempDirectory("flink");
    List<String> flink = runToFile(Pipeline.create(opts), d2);

    assertEquals(direct.size(), flink.size());
    assertEquals(direct, flink);
    // 400 distinct ids over 10 one-minute windows and 4 users
    assertEquals(true, direct.size() > 0 && direct.size() <= 40);
  }
}
