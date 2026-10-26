package lab;

import java.io.Serializable;
import java.util.Objects;

/** One purchase event. The id lets us drop duplicate deliveries. */
public final class Event implements Serializable {
  public final String id;
  public final String user;
  public final long ts; // event time, epoch millis
  public final long amount;

  public Event(String id, String user, long ts, long amount) {
    this.id = id;
    this.user = user;
    this.ts = ts;
    this.amount = amount;
  }

  public static Event parse(String line) {
    String[] p = line.split(",");
    return new Event(p[0], p[1], Long.parseLong(p[2]), Long.parseLong(p[3]));
  }

  @Override public boolean equals(Object o) {
    return o instanceof Event e && id.equals(e.id) && user.equals(e.user) && ts == e.ts && amount == e.amount;
  }

  @Override public int hashCode() { return Objects.hash(id, user, ts, amount); }

  @Override public String toString() { return id + "," + user + "," + ts + "," + amount; }
}
