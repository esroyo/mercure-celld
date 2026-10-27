import { assertEquals } from "jsr:@std/assert";
import { SseUpdate } from "./sse_update.ts";
import { SseUpdatePost, SseUpdateRow } from "./types.ts";

Deno.test("should take eventData as-is", async (_t) => {
  const update = new SseUpdate({ eventData: ["potato"], topic: "default" });
  assertEquals(update.eventData, ["potato"]);
});

Deno.test("should default id value to an ulid", async (_t) => {
  const update = new SseUpdate({ eventData: ["potato"], topic: "default" });
  assertEquals(update.id.length, 26);
});

Deno.test("should take id value as-is", async (_t) => {
  const update = new SseUpdate({
    eventData: ["potato"],
    topic: "default",
    id: "x",
  });
  assertEquals(update.id, "x");
});

Deno.test("should default eventId value to null", async (_t) => {
  const update = new SseUpdate({ eventData: ["potato"], topic: "default" });
  assertEquals(update.eventId, null);
});

Deno.test("should take eventId value as-is", async (_t) => {
  const update = new SseUpdate({
    eventData: ["potato"],
    topic: "default",
    eventId: "y",
  });
  assertEquals(update.eventId, "y");
});

Deno.test("should default eventType value to null", async (_t) => {
  const update = new SseUpdate({ eventData: ["potato"], topic: "default" });
  assertEquals(update.eventType, null);
});

Deno.test("should take eventType value as-is", async (_t) => {
  const update = new SseUpdate({
    eventData: ["potato"],
    topic: "default",
    eventType: "add",
  });
  assertEquals(update.eventType, "add");
});

Deno.test("should serialize an event without optionals as expected", async (t) => {
  const update = new SseUpdate({ eventData: ["potato"], topic: "default" }, {
    ulid: () => "ULID",
  });
  await t.assertSnapshot(update.toString());
});

Deno.test("should serialize an event with eventType field as expected", async (t) => {
  const update = new SseUpdate({
    eventData: ["potato"],
    topic: "default",
    eventType: "add",
  }, { ulid: () => "ULID" });
  await t.assertSnapshot(update.toString());
});

Deno.test("should serialize an event with eventRetry field as expected", async (t) => {
  const update = new SseUpdate({
    eventData: ["potato"],
    topic: "default",
    eventRetry: 5,
  }, { ulid: () => "ULID" });
  await t.assertSnapshot(update.toString());
});

Deno.test("should serialize an event with multiple data lines as expected", async (t) => {
  const update = new SseUpdate({
    eventData: ["potato", "garlic"],
    topic: "default",
  }, { ulid: () => "ULID" });
  await t.assertSnapshot(update.toString());
});

Deno.test("should map Publish body options", async (t) => {
  const postBody: SseUpdatePost = {
    topics: [{ match: "greeting" }],
    data: ["hello", "world"],
    id: "1-1",
    retry: "5",
    type: "message",
    private: "",
  };
  const update = await SseUpdate.from(postBody);
  assertEquals(update.topic, postBody.topics[0].match);
  assertEquals(update.eventData, postBody.data);
  assertEquals(update.eventId, postBody.id);
  assertEquals(update.eventRetry, Number(postBody.retry));
  assertEquals(update.eventType, postBody.type);
  assertEquals(update.private, 0);
  await t.assertSnapshot(update.toString());
});

Deno.test("should map DB row columns", async (t) => {
  const row: SseUpdateRow = {
    id: "ULID",
    event_data: "hello\nworld",
    event_id: "1-1",
    event_retry: 5,
    event_type: "message",
    private: 0,
  };
  const update = await SseUpdate.from({ ...row, topic: "x" });
  assertEquals(update.topic, "x");
  assertEquals(update.eventData, row.event_data.split("\n"));
  assertEquals(update.eventId, row.event_id);
  assertEquals(update.eventRetry, row.event_retry);
  assertEquals(update.eventType, row.event_type);
  assertEquals(update.private, row.private);
  assertEquals(update.id, row.id);
  await t.assertSnapshot(update.toString());
});

Deno.test("should map DB row null columns", async (t) => {
  const row: SseUpdateRow = {
    id: "ULID",
    event_data: null,
    event_id: null,
    event_retry: null,
    event_type: null,
    private: 0,
  };
  const update = await SseUpdate.from({ ...row, topic: "x" });
  assertEquals(update.topic, "x");
  assertEquals(update.eventData, []);
  assertEquals(update.eventId, row.event_id);
  assertEquals(update.eventRetry, row.event_retry);
  assertEquals(update.eventType, row.event_type);
  assertEquals(update.private, row.private);
  assertEquals(update.id, row.id);
  await t.assertSnapshot(update.toString());
});
