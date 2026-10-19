import { assertEquals } from "@std/assert/equals";
import { Subscriber } from "./subscriber.ts";

Deno.test("should take the topic value from the same param", async (_t) => {
  const subscriber = await Subscriber.from(
    new Request("http://localhost/?topic=example"),
  );
  assertEquals(subscriber.topic, "example");
});

Deno.test("should default the lastEventId value to null", async (_t) => {
  const subscriber = await Subscriber.from(
    new Request("http://localhost/?topic=example"),
  );
  assertEquals(subscriber.lastEventId, null);
});

Deno.test("should take the lastEventId value from header `Last-Event-Id (priority 3)", async (_t) => {
  const subscriber = await Subscriber.from(
    new Request("http://localhost/?topic=example", {
      headers: { "Last-Event-Id": "x" },
    }),
  );
  assertEquals(subscriber.lastEventId, "x");
});

Deno.test("should take the lastEventId value from the request param `lastEventID` (priority 2)", async (_t) => {
  const subscriber = await Subscriber.from(
    new Request("http://localhost/?topic=example&lastEventID=y", {
      headers: { "Last-Event-Id": "x" },
    }),
  );
  assertEquals(subscriber.lastEventId, "y");
});

Deno.test("should take the lastEventId value from the request param `lastEventId` (priority 1)", async (_t) => {
  const subscriber = await Subscriber.from(
    new Request("http://localhost/?topic=example&lastEventId=z&lastEventID=y", {
      headers: { "Last-Event-Id": "x" },
    }),
  );
  assertEquals(subscriber.lastEventId, "z");
});

Deno.test("should take the lastEventId value from the request param `last_event_id` (priority 0)", async (_t) => {
  const subscriber = await Subscriber.from(
    new Request(
      "http://localhost/?topic=example&last_event_id=zz&lastEventId=z&lastEventID=y",
      {
        headers: { "Last-Event-Id": "x" },
      },
    ),
  );
  assertEquals(subscriber.lastEventId, "zz");
});
