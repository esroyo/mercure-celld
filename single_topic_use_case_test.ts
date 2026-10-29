import { assert, assertEquals } from "@std/assert";
import { SingleTopicUseCase } from "./single_topic_use_case.ts";
import { DatabaseSync } from "node:sqlite";
import { assertSpyCalls, spy } from "@std/testing/mock";
import { stub } from "@std/testing/unstable-stub";
import { SubscriberSingleTopic } from "./subscriber.ts";

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

function createEnv(): { ctx: DurableObjectState; db: DatabaseSync; env: Env } {
  const { db, sql } = createSql();
  const env = {} as unknown as Env;
  const ctx = {
    id: { name: topicName },
    storage: { sql },
  } as unknown as DurableObjectState;
  return { db, ctx, env };
}

const STEP_TIMEOUT = 250;
function createReader(stream: ReadableStream) {
  return stream.pipeThrough(new TextDecoderStream()).getReader();
}

function readChunk(
  reader: ReadableStreamDefaultReader<string>,
  label = "a frame",
): Promise<ReadableStreamReadResult<string>> {
  const deferred = Promise.withResolvers<ReadableStreamReadResult<string>>();
  const timer: ReturnType<typeof setTimeout> = setTimeout(() => {
    deferred.reject(new Error(`timed out waiting for ${label}`));
  }, STEP_TIMEOUT);
  reader.read()
    .then(deferred.resolve, deferred.reject)
    .finally(() => clearTimeout(timer));
  return deferred.promise;
}

const createPostRequest = (data: Array<[string, string]> = []) =>
  new Request("http://localhost/", {
    method: "POST",
    body: new URLSearchParams([["topic", topicName], ...data]),
    headers: { "content-type": "application/x-www-form-urlencoded" },
  });

Deno.test("should subscribe from now", async (_t) => {
  const { ctx, env } = createEnv();
  const useCase = new SingleTopicUseCase(ctx, env);

  await useCase.fetch(createPostRequest([["data", "before"]]));

  const response = await useCase.fetch(
    new Request(`http://localhost/?match=${topicName}`),
  );
  const reader = createReader(response.body);

  await useCase.fetch(createPostRequest([["data", "after1"]]));
  await useCase.fetch(createPostRequest([["data", "after2"]]));

  assert((await readChunk(reader, "frame 1")).value.includes("data: after1"));
  assert((await readChunk(reader, "frame 2")).value.includes("data: after2"));
});

Deno.test("should subscribe from the earliest update", async (_t) => {
  const { ctx, env } = createEnv();
  const useCase = new SingleTopicUseCase(ctx, env);

  await useCase.fetch(createPostRequest([["data", "before1"]]));
  await useCase.fetch(createPostRequest([["data", "before2"]]));

  const response = await useCase.fetch(
    new Request(`http://localhost/?match=${topicName}&lastEventId=earliest`),
  );
  const reader = createReader(response.body);

  await useCase.fetch(createPostRequest([["data", "after1"]]));

  assert((await readChunk(reader, "frame 1")).value.includes("data: before1"));
  assert((await readChunk(reader, "frame 2")).value.includes("data: before2"));
  assert((await readChunk(reader, "frame 3")).value.includes("data: after1"));
});

Deno.test("should subscribe from an specific update (built-in id)", async (_t) => {
  const { ctx, env } = createEnv();
  const useCase = new SingleTopicUseCase(ctx, env);

  const spyResponse = await useCase.fetch(
    new Request(`http://localhost/?match=${topicName}`),
  );
  const spyReader = createReader(spyResponse.body);

  await useCase.fetch(createPostRequest([["data", "x1"]]));
  await useCase.fetch(createPostRequest([["data", "x2"]]));
  await useCase.fetch(createPostRequest([["data", "x3"]]));

  await readChunk(spyReader, "frame 1");
  const [_, id] = (await readChunk(spyReader, "frame 2")).value.match(
    /id: ([\S]+)/,
  );

  const response = await useCase.fetch(
    new Request(
      `http://localhost/?match=${topicName}&lastEventId=${
        encodeURIComponent(id)
      }`,
    ),
  );
  const reader = createReader(response.body);
  assert((await readChunk(reader, "frame 3")).value.includes("data: x3"));
});

Deno.test("should subscribe from an specific update (custom id)", async (_t) => {
  const { ctx, env } = createEnv();
  const useCase = new SingleTopicUseCase(ctx, env);

  const eventId = "magic-potato-1";
  await useCase.fetch(createPostRequest([["data", "x1"]]));
  await useCase.fetch(createPostRequest([["data", "x2"], ["id", eventId]]));
  await useCase.fetch(createPostRequest([["data", "x3"]]));

  const response = await useCase.fetch(
    new Request(`http://localhost/?match=${topicName}&lastEventId=${eventId}`),
  );
  const reader = createReader(response.body);
  assert((await readChunk(reader, "frame 1")).value.includes("data: x3"));
});

Deno.test("should subscribe from now if the specific update is unknown", async (_t) => {
  const { ctx, env } = createEnv();
  const useCase = new SingleTopicUseCase(ctx, env);

  const eventId = "magic-potato-1";
  await useCase.fetch(createPostRequest([["data", "x1"]]));
  await useCase.fetch(createPostRequest([["data", "x2"]]));

  const response = await useCase.fetch(
    new Request(`http://localhost/?match=${topicName}&lastEventId=${eventId}`),
  );
  const reader = createReader(response.body);

  await useCase.fetch(createPostRequest([["data", "x3"]]));
  await useCase.fetch(createPostRequest([["data", "x4"]]));

  assert((await readChunk(reader, "frame 1")).value.includes("data: x3"));
  assert((await readChunk(reader, "frame 2")).value.includes("data: x4"));
});

Deno.test("should not receive private updates if not authorized", async (_t) => {
  const { ctx, env } = createEnv();
  using _ = stub(
    SubscriberSingleTopic.prototype,
    "isAuthorized",
    async () => false,
  );
  const useCase = new SingleTopicUseCase(ctx, env);

  const response = await useCase.fetch(
    new Request(`http://localhost/?match=${topicName}`),
  );
  const reader = createReader(response.body);

  await useCase.fetch(createPostRequest([["data", "x1"], ["private", "on"]]));
  await useCase.fetch(createPostRequest([["data", "x2"]]));

  assert((await readChunk(reader, "frame 1")).value.includes("data: x2"));
});

Deno.test("should receive private updates if authorized", async (_t) => {
  const { ctx, env } = createEnv();
  using _ = stub(
    SubscriberSingleTopic.prototype,
    "isAuthorized",
    async () => true,
  );
  const useCase = new SingleTopicUseCase(ctx, env);

  const response = await useCase.fetch(
    new Request(`http://localhost/?match=${topicName}`),
  );
  const reader = createReader(response.body);

  await useCase.fetch(createPostRequest([["data", "x1"], ["private", "on"]]));
  await useCase.fetch(createPostRequest([["data", "x2"]]));

  assert((await readChunk(reader, "frame 1")).value.includes("data: x1"));
  assert((await readChunk(reader, "frame 2")).value.includes("data: x2"));
});

Deno.test("should unsubscribe if stream closes", async (_t) => {
  const { ctx, env } = createEnv();
  const useCase = new SingleTopicUseCase(ctx, env);

  const ctrl = new AbortController();
  {
    const response = await useCase.fetch(
      new Request(`http://localhost/?match=${topicName}`, {
        signal: ctrl.signal,
      }),
    );
    const _ = createReader(response.body);
  }

  const response = await useCase.fetch(
    new Request(`http://localhost/?match=${topicName}`),
  );
  const reader = createReader(response.body);

  assertEquals(useCase.subscribers.size, 2);
  ctrl.abort(new Error("Bye"));

  await useCase.fetch(createPostRequest([["data", "x1"]]));

  assert((await readChunk(reader, "frame 1")).value.includes("data: x1"));
  assertEquals(useCase.subscribers.size, 1);
});
