import { DatabaseSync } from "node:sqlite";
import { assertEquals, assertRejects } from "@std/assert";
import { assertSpyCalls, spy } from "@std/testing/mock";
import { stub } from "@std/testing/unstable-stub";

import { Topic } from "./topic.ts";
import { SseUpdate } from "./sse_update.ts";
import { Subscriber } from "./subscriber.ts";

function createSql() {
  const db = new DatabaseSync(":memory:");
  const sql = {
    exec: spy((query: string, ...bindings: any[]) => {
      const rows: any[] = db.prepare(query).all(...bindings);
      return { toArray: () => rows };
    }),
  };
  return { db, sql };
}

const topicName = "test";

Deno.test("should create once the sse_updates table on construction", async (_t) => {
  const { sql } = createSql();
  {
    const _ = new Topic({ name: topicName }, { sql });
    const result = sql.exec(
      "SELECT name FROM sqlite_schema WHERE type = 'table'",
    ).toArray();
    assertEquals(result.length, 1);
    assertEquals(result[0].name, "sse_updates");
  }

  {
    const _ = new Topic({ name: topicName }, { sql });
    const result = sql.exec(
      "SELECT name FROM sqlite_schema WHERE type = 'table'",
    ).toArray();
    assertEquals(result.length, 1);
    assertEquals(result[0].name, "sse_updates");
  }
  assertSpyCalls(sql.exec, 2 + 2);
});

Deno.test("should persist SseUpdates on sql, across different Topic instances", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  const update = new SseUpdate({
    eventId: "1",
    eventData: ["foobar"],
    eventRetry: 5,
    eventType: "message",
    private: 0,
    topic: topicName,
  });
  await topic.addSseUpdate(update);
  const anotherUpdate = new SseUpdate({ topic: topicName });
  await topic.addSseUpdate(anotherUpdate);
  const found = await topic.findSseUpdate();
  assertEquals(found.length, 2);
  assertEquals(JSON.stringify(found[0]), JSON.stringify(update));
  assertEquals(JSON.stringify(found[1]), JSON.stringify(anotherUpdate));
  const topicSame = new Topic({ name: topicName }, { sql });
  assertEquals((await topicSame.findSseUpdate()).length, 2);
});

Deno.test("should reject duplicate event_id updates", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  const eventId = crypto.randomUUID();
  const update = new SseUpdate({ eventId, topic: topicName });
  await topic.addSseUpdate(update);
  assertRejects(() => topic.addSseUpdate(update));
});

Deno.test("should find by event_id", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  const eventId = crypto.randomUUID();
  const update = new SseUpdate({ eventId, topic: topicName });
  await topic.addSseUpdate(update);
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const [found] = await topic.findSseUpdate({ where: { event_id: eventId } });
  assertEquals(JSON.stringify(found), JSON.stringify(update));
});

Deno.test("should find by id gt filter", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const update = new SseUpdate({ topic: topicName });
  await topic.addSseUpdate(update);
  const anotherUpdate = new SseUpdate({ topic: topicName });
  await topic.addSseUpdate(anotherUpdate);
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const found = await topic.findSseUpdate({ where: { id: { gt: update.id } } });
  assertEquals(found.length, 2);
  assertEquals(JSON.stringify(found[0]), JSON.stringify(anotherUpdate));
});

Deno.test("should subscribe from now", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const subscriber = new Subscriber({ topic: topicName });
  const sendSpy = spy(subscriber, "send");
  await topic.addSubscriber(subscriber);
  assertSpyCalls(sendSpy, 0);
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  assertSpyCalls(sendSpy, 2);
});

Deno.test("should subscribe from the earliest update", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const subscriber = new Subscriber({ lastEventId: "", topic: topicName });
  const sendSpy = spy(subscriber, "send");
  await topic.addSubscriber(subscriber);
  assertSpyCalls(sendSpy, 3);
});

Deno.test("should subscribe from an specific update (built-in id)", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const update = new SseUpdate({ topic: topicName });
  await topic.addSseUpdate(update);
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const subscriber = new Subscriber({
    lastEventId: `#${update.id}`,
    topic: topicName,
  });
  const sendSpy = spy(subscriber, "send");
  await topic.addSubscriber(subscriber);
  assertSpyCalls(sendSpy, 1);
});

Deno.test("should subscribe from an specific update (custom id)", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const eventId = "magic-potato-1";
  const update = new SseUpdate({ eventId, topic: topicName });
  await topic.addSseUpdate(update);
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const subscriber = new Subscriber({ lastEventId: eventId, topic: topicName });
  const sendSpy = spy(subscriber, "send");
  await topic.addSubscriber(subscriber);
  assertSpyCalls(sendSpy, 1);
});

Deno.test("should subscribe from now if the specific update is unknown", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  const subscriber = new Subscriber({
    lastEventId: "magic-potato-1",
    topic: topicName,
  });
  const sendSpy = spy(subscriber, "send");
  await topic.addSubscriber(subscriber);
  assertSpyCalls(sendSpy, 0);
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  assertSpyCalls(sendSpy, 1);
});

Deno.test("should not receive private updates if not authorized", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  const subscriber = new Subscriber({ topic: topicName });
  const sendSpy = spy(subscriber, "send");
  const _ = stub(subscriber, "isAuthorized", async () => false);
  await topic.addSubscriber(subscriber);
  assertSpyCalls(sendSpy, 0);
  const privateUpdate = new SseUpdate({ topic: topicName, private: 1 });
  await topic.addSseUpdate(privateUpdate);
  assertSpyCalls(sendSpy, 0);
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  assertSpyCalls(sendSpy, 1);
});

Deno.test("should receive private updates if authorized", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  const subscriber = new Subscriber({ topic: topicName });
  const sendSpy = spy(subscriber, "send");
  const _ = stub(subscriber, "isAuthorized", async () => true);
  await topic.addSubscriber(subscriber);
  assertSpyCalls(sendSpy, 0);
  const privateUpdate = new SseUpdate({ topic: topicName, private: 1 });
  await topic.addSseUpdate(privateUpdate);
  assertSpyCalls(sendSpy, 1);
  await topic.addSseUpdate(new SseUpdate({ topic: topicName }));
  assertSpyCalls(sendSpy, 2);
});

Deno.test("should unsubscribe if stream closes", async (_t) => {
  const { sql } = createSql();
  const topic = new Topic({ name: topicName }, { sql });
  const subscriber = new Subscriber({ topic: topicName });
  const deferredClosed = Promise.withResolvers();
  const _ = stub(subscriber, "closed", { get: () => deferredClosed.promise });
  const sendSpy = spy(subscriber, "send");
  deferredClosed.reject();
  await topic.addSubscriber(subscriber);
  assertSpyCalls(sendSpy, 0);
});
