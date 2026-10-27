import { assert, assertEquals } from "@std/assert";
import { MultiTopicUseCase } from "./multi_topic_use_case.ts";

const enc = new TextEncoder();

function createUpstream(frames: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const frame of frames) controller.enqueue(enc.encode(frame));
      controller.close();
    },
  });
}

function createResponse(frames: string[]): Response {
  return new Response(createUpstream(frames), {
    headers: { "content-type": "text/event-stream" },
  });
}

type DoCall = { name: string; request: Request };

function createEnv(
  handle: (name: string) => Response,
): { calls: DoCall[]; env: Env } {
  const calls: DoCall[] = [];
  const env = {
    SINGLE_TOPIC_USE_CASE: {
      idFromName: (name: string) => ({ name }),
      get: (id: { name: string }) => ({
        fetch: (request: Request) => {
          calls.push({ name: id.name, request });
          return handle(id.name);
        },
      }),
    },
  } as unknown as Env;
  return { calls, env };
}

Deno.test("should wire exactly one connect per unique match", async () => {
  const { calls, env } = createEnv(() =>
    createResponse(["id: 1\ndata: hi\n\n"])
  );
  const useCase = new MultiTopicUseCase({}, env);
  const response = await useCase.fetch(
    new Request("http://localhost/?match=a&match=b"),
  );
  const subscriber = Array.from(useCase.subscribers)[0];

  assert(subscriber.topics.every((t) => t.isConnected));
  assertEquals(response.headers.get("content-type"), "text/event-stream");
  assertEquals(
    calls.map((c) => c.name),
    ["a", "b"],
    "should fetch single-topic DO once per unique match",
  );
});

Deno.test("should fail before wiring any connect when a single-topic DO response is not ok", async () => {
  const { env } = createEnv((name) =>
    name === "b"
      ? new Response("topic broke", { status: 503 })
      : createResponse(["id: 1\ndata: a\n\n"])
  );
  const useCase = new MultiTopicUseCase({}, env);
  const response = await useCase.fetch(
    new Request("http://localhost/?match=a&match=b"),
  );

  assertEquals(response.status, 503);
  assertEquals(useCase.subscribers.size, 0);
});

Deno.test("should fail before wiring any connect when there is a network error", async () => {
  const { env } = createEnv((name) => {
    if (name === "b") throw new TypeError("Network request failed");
    return createResponse(["id: 1\ndata: a\n\n"]);
  });
  const useCase = new MultiTopicUseCase({}, env);
  const response = await useCase.fetch(
    new Request("http://localhost/?match=a&match=b"),
  );

  assertEquals(response.status, 502);
  assertEquals(useCase.subscribers.size, 0);
});

Deno.test("should forward headers, method, signal and match param to single-topic DO fetches", async () => {
  const { calls, env } = createEnv(() => createResponse([]));
  const useCase = new MultiTopicUseCase({}, env);
  const controller = new AbortController();
  const request = new Request("http://localhost/sub?match=a&match=b", {
    headers: { "authorization": "Bearer token" },
    method: "GET",
    signal: controller.signal,
  });
  await useCase.fetch(request);
  const subscriber = Array.from(useCase.subscribers)[0];

  assertEquals(calls.length, 2);
  calls.forEach((call, idx) => {
    assertEquals(
      new URL(call.request.url).searchParams.get("match"),
      subscriber.topics[idx].match,
    );
    assertEquals(
      call.request.headers.get("authorization"),
      "Bearer token",
    );
    assertEquals(call.request.method, "GET");
    // Deno reconstructs the signal object through Request construction, so identity
    // is not preserved; the teardown contract is abort propagation.
    assert(!call.request.signal.aborted);
  });

  controller.abort();
  await new Promise((resolve) => queueMicrotask(resolve));
  calls.forEach((call) => {
    assertEquals(call.request.signal.aborted, true);
  });
});
