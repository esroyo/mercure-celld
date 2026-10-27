import { assert, assertEquals, assertRejects } from "@std/assert";
import {
  Subscriber,
  SubscriberMultiTopic,
  SubscriberSingleTopic,
} from "./subscriber.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();
const STEP_TIMEOUT = 250;

function createUpstream(chunks: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(enc.encode(chunk));
      }
      controller.close();
    },
  });
}

function createControlledUpstream() {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  const stream = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
    },
  });
  return {
    stream,
    enqueue(text: string) {
      controller.enqueue(enc.encode(text));
    },
    close() {
      controller.close();
    },
    error(reason: unknown) {
      controller.error(reason);
    },
  };
}

function createReader(sub: SubscriberMultiTopic) {
  return sub.getReadableStream().pipeThrough(new TextDecoderStream())
    .getReader();
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

Deno.test("[Subscriber] should keep the full topics list and expose the first as topic", async () => {
  const subscriber = await Subscriber.from(
    new Request("http://localhost/?match=a&match=b"),
  );
  assertEquals(subscriber.topics, [{ match: "a" }, { match: "b" }]);
  assertEquals(subscriber.topic, "a");
});

Deno.test.each([
  ["multiple exact topics", "/?match=a&match=b"],
  ["duped exact topic", "/?match=a&match=a"],
  ["non-exact topic", "/?match_urlpattern=foo*"],
  ["exact topic mixed with non-exact topic", "/?match=a&match_urlpattern=foo*"],
])(
  "[SubscriberSingleTopic] should not construct when given %s",
  async (_desc, params) => {
    await assertRejects(() =>
      SubscriberSingleTopic.from(
        new Request(new URL(params, "http://localhost")),
      )
    );
  },
);

Deno.test("[SubscriberSingleTopic] should construct when given an exact topic", async () => {
  const _subscriber = await SubscriberSingleTopic.from(
    new Request("http://localhost/?match=a"),
  );
});

Deno.test.each([
  ["single exact topic", "/?match=a"],
  ["duped exact topic", "/?match=a&match=a"],
  ["duped exact topics", "/?match=a&match=b&match=a"],
  ["non-exact topic", "/?match_urlpattern=foo*"],
  [
    "exact topic mixed with non-exact topic",
    "/?match=a&match=b&match_urlpattern=foo*",
  ],
])(
  "[SubscriberMultiTopic] should not construct when given %s",
  async (_desc, params) => {
    await assertRejects(() =>
      SubscriberMultiTopic.from(
        new Request(new URL(params, "http://localhost")),
      )
    );
  },
);

Deno.test.each([
  ["2 exact topics", "/?match=a&match=b"],
  ["+2 exact topics", "/?match=a&match=b&match=c"],
])(
  "[SubscriberMultiTopic] should construct when given %s",
  async (_desc, params) => {
    const _subscriber = await SubscriberMultiTopic.from(
      new Request(new URL(params, "http://localhost")),
    );
  },
);

Deno.test("[SubscriberMultiTopic] forwards frames verbatim across chunk splits and keeps an unterminated tail", async () => {
  const subscriber = new SubscriberMultiTopic({
    topics: [{ match: "a" }, { match: "b" }],
  });
  const chunks = ["id: 1\ndata: a\n\n", "data: b\n", "\n", "data: tail-part"];
  const promise = subscriber.connect("a", createUpstream(chunks));
  const reader = createReader(subscriber);

  assertEquals((await readChunk(reader, "frame 1")).value, chunks[0]);
  assertEquals(
    (await readChunk(reader, "frame 2")).value,
    chunks[1] + chunks[2],
  );
  assertEquals((await readChunk(reader, "tail")).value, chunks[3]);
  assert((await readChunk(reader, "close")).done);

  await promise;
});

Deno.test("[SubscriberMultiTopic] closes the merged stream when its sole upstream drains", async () => {
  const subcriber = new SubscriberMultiTopic({
    topics: [{ match: "a" }, { match: "b" }],
  });
  const chunks = ["id: 1\ndata: a\n\n"];
  const promise = subcriber.connect("a", createUpstream(chunks));
  const reader = createReader(subcriber);

  assertEquals((await readChunk(reader)).value, chunks[0]);
  assert((await readChunk(reader, "close")).done);

  await promise;
});

Deno.test("[SubscriberMultiTopic] keeps the merged stream open when one upstream EOFs while a sibling is live", async () => {
  const subscriber = new SubscriberMultiTopic({
    topics: [{ match: "a" }, { match: "b" }],
  });
  const second = createControlledUpstream();
  const chunks = ["id: 1\ndata: a\n\n"];
  const firstPromise = subscriber.connect("a", createUpstream(chunks));
  const secondPromise = subscriber.connect("b", second.stream);
  const reader = createReader(subscriber);

  const frame = await readChunk(reader);
  assert(!frame.done);
  assertEquals(frame.value, chunks[0]);

  const result = await Promise.race([
    reader.read()
      .then((x) => x.done ? "closed" : "frame")
      .catch(() => "error"),
    new Promise<string>((resolve) => setTimeout(() => resolve("open"), 50)),
  ]);
  assertEquals(result, "open");

  await reader.cancel();
  await secondPromise;
  await firstPromise;
});

Deno.test("[SubscriberMultiTopic] closes the merged stream only once every upstream drains", async () => {
  const subscriber = new SubscriberMultiTopic({
    topics: [{ match: "a" }, { match: "b" }],
  });
  const second = createControlledUpstream();
  const chunks = ["id: 1\ndata: a\n\n"];
  const firstPromise = subscriber.connect("a", createUpstream(chunks));
  const secondPromise = subscriber.connect("b", second.stream);
  const reader = createReader(subscriber);

  assertEquals((await readChunk(reader, "frame 1")).value, chunks[0]);
  await firstPromise;

  const chunk = "id: 2\ndata: b\n\n";
  second.enqueue(chunk);
  assertEquals((await readChunk(reader, "frame 2")).value, chunk);

  second.close();
  assert((await readChunk(reader, "close")).done);

  await secondPromise;
});

Deno.test("[SubscriberMultiTopic] aborts the merged stream when one upstream errors (fail-all)", async () => {
  const subscriber = new SubscriberMultiTopic({
    topics: [{ match: "a" }, { match: "b" }],
  });
  const first = createControlledUpstream();
  const second = createControlledUpstream();
  const firstPromise = subscriber.connect("a", first.stream);
  const secondPromise = subscriber.connect("b", second.stream);
  const reader = createReader(subscriber);

  first.error(new Error("boom"));

  await assertRejects(() => reader.read(), Error, "boom");
  await Promise.all([firstPromise, secondPromise]);
});

Deno.test("[SubscriberMultiTopic] ignores a duplicate connect for the same topic", async () => {
  const subscriber = new SubscriberMultiTopic({
    topics: [{ match: "a" }, { match: "b" }],
  });
  const chunks = ["id: 1\ndata: x\n\n"];
  const firstPromise = subscriber.connect("a", createUpstream(chunks));
  const secondPromise = subscriber.connect(
    "a",
    createUpstream(["id: 2\ndata: y\n\n"]),
  );
  const read = (await Array.fromAsync(
    subscriber.getReadableStream(),
    (value) => dec.decode(value),
  )).join("");
  assertEquals(read, chunks.join(""));
  await firstPromise;
  await secondPromise;
});
