import { assert, assertEquals, assertRejects } from "@std/assert";
import { SseStreamer } from "./sse_streamer.ts";

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

function createReader(
  streamer: SseStreamer,
  options: { preventCancel?: boolean } = {},
) {
  return streamer.getReadableStream(options).pipeThrough(
    new TextDecoderStream(),
  ).getReader();
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

Deno.test("should forward frames verbatim across chunk splits and keep an unterminated tail", async () => {
  const sse = new SseStreamer();
  const chunks = ["id: 1\ndata: a\n\n", "data: b\n", "\n", "data: tail-part"];
  const promise = sse.take(createUpstream(chunks));
  const reader = createReader(sse);

  assertEquals((await readChunk(reader, "frame 1")).value, chunks[0]);
  assertEquals(
    (await readChunk(reader, "frame 2")).value,
    chunks[1] + chunks[2],
  );
  assertEquals((await readChunk(reader, "tail")).value, chunks[3]);
  assert((await readChunk(reader, "close")).done);

  await promise;
});

Deno.test("should close the merged stream when its sole upstream drains", async () => {
  const sse = new SseStreamer();
  const chunks = ["id: 1\ndata: a\n\n"];
  const promise = sse.take(createUpstream(chunks));
  const reader = createReader(sse);

  assertEquals((await readChunk(reader)).value, chunks[0]);
  assert((await readChunk(reader, "close")).done);

  await promise;
});

Deno.test("should keep the merged stream open when one upstream EOFs while a sibling is live", async () => {
  const sse = new SseStreamer();
  const second = createControlledUpstream();
  const chunks = ["id: 1\ndata: a\n\n"];
  const firstPromise = sse.take(createUpstream(chunks));
  const secondPromise = sse.take(second.stream);
  const reader = createReader(sse);

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

Deno.test("should close the merged stream only once every upstream drains", async () => {
  const sse = new SseStreamer();
  const second = createControlledUpstream();
  const chunks = ["id: 1\ndata: a\n\n"];
  const firstPromise = sse.take(createUpstream(chunks));
  const secondPromise = sse.take(second.stream);
  const reader = createReader(sse);

  assertEquals((await readChunk(reader, "frame 1")).value, chunks[0]);
  await firstPromise;

  const chunk = "id: 2\ndata: b\n\n";
  second.enqueue(chunk);
  assertEquals((await readChunk(reader, "frame 2")).value, chunk);

  second.close();
  assert((await readChunk(reader, "close")).done);

  await secondPromise;
});

Deno.test("should abort the merged stream when one upstream errors (fail-all)", async () => {
  const sse = new SseStreamer();
  const first = createControlledUpstream();
  const second = createControlledUpstream();
  const firstPromise = sse.take(first.stream);
  const secondPromise = sse.take(second.stream);
  const reader = createReader(sse);

  first.error(new Error("boom"));

  await assertRejects(() => reader.read(), Error, "boom");
  await Promise.all([firstPromise, secondPromise]);
});

Deno.test("should ignore a duplicate take", async () => {
  const sse = new SseStreamer();
  const chunks = ["id: 1\ndata: x\n\n"];
  const upstream = createUpstream(chunks);
  const firstPromise = sse.take(upstream);
  const secondPromise = sse.take(upstream);
  const read = (await Array.fromAsync(
    sse.getReadableStream(),
    (value) => dec.decode(value),
  )).join("");
  assertEquals(read, chunks.join(""));
  await firstPromise;
  await secondPromise;
});

Deno.test("should not replay frames emitted before a consumer joined", async () => {
  const sse = new SseStreamer();
  await sse.emit("id: 0\ndata: old\n\n");

  const reader = createReader(sse);
  await sse.emit("id: 1\ndata: new\n\n");

  const postJoin = "id: 1\ndata: new\n\n";
  assertEquals((await readChunk(reader, "post-join frame")).value, postJoin);

  await sse.close();
  assert((await readChunk(reader, "close")).done);
});

Deno.test("should keep a streamer alive across consumer rotation with preventCancel", async () => {
  const sse = new SseStreamer();
  const first = createReader(sse, { preventCancel: true });
  await first.cancel();
  await sse.emit("id: 1\ndata: x\n\n");

  const second = createReader(sse);
  await sse.emit("id: 2\ndata: y\n\n");
  assertEquals(
    (await readChunk(second, "post-rotation frame")).value,
    "id: 2\ndata: y\n\n",
  );

  await sse.close();
  assert((await readChunk(second, "close")).done);
});

Deno.test("should survive upstream drain gaps with preventClose", async () => {
  const sse = new SseStreamer();
  const firstPromise = sse.take(
    createUpstream(["id: 1\ndata: a\n\n"]),
    { preventClose: true },
  );
  const reader = createReader(sse);
  assertEquals(
    (await readChunk(reader, "gap frame")).value,
    "id: 1\ndata: a\n\n",
  );
  await firstPromise;

  const secondPromise = sse.take(
    createUpstream(["id: 2\ndata: b\n\n"]),
    { preventClose: true },
  );
  assertEquals(
    (await readChunk(reader, "post-gap frame")).value,
    "id: 2\ndata: b\n\n",
  );
  await secondPromise;

  await sse.close();
  assert((await readChunk(reader, "close")).done);
});

Deno.test("should propagate close() through composition as a clean end", async () => {
  const frame = "id: 1\ndata: x\n\n";
  const parent = new SseStreamer();
  const feed = createControlledUpstream();
  const childOne = new SseStreamer();
  const childTwo = new SseStreamer();
  const feedPromise = parent.take(feed.stream);
  const onePromise = childOne.take(parent.getReadableStream());
  const twoPromise = childTwo.take(parent.getReadableStream());
  const readerOne = createReader(childOne);
  const readerTwo = createReader(childTwo);

  feed.enqueue(frame);
  assertEquals(
    (await readChunk(readerOne, "frame - child 1")).value,
    frame,
  );
  assertEquals(
    (await readChunk(readerTwo, "frame - child 2")).value,
    frame,
  );

  await parent.close();
  assert((await readChunk(readerOne, "close - child 1")).done);
  assert((await readChunk(readerTwo, "close - child 2")).done);
  feed.close();
  await Promise.all([feedPromise, onePromise, twoPromise]);
});

Deno.test("should close a bare streamer and expose the terminal state to late joiners", async () => {
  const sse = new SseStreamer();
  const reader = createReader(sse);

  await sse.close();
  assert((await readChunk(reader, "close")).done);

  const late = createReader(sse);
  assert((await readChunk(late, "late close")).done);
});

Deno.test("should abort a bare streamer with a reason and still resolve late takes", async () => {
  const sse = new SseStreamer();
  const reader = createReader(sse);
  await sse.abort(new Error("reason"));
  await assertRejects(() => readChunk(reader, "abort"), Error, "reason");

  const late = createReader(sse);
  await assertRejects(() => readChunk(late, "late join"), Error, "reason");

  await sse.take(createUpstream(["id: 1\ndata: x\n\n"]));
});

Deno.test("should be able to compose streamers", async () => {
  const chunks = ["id: 1\ndata: x\n\n"];
  const controller = new AbortController();
  const original = new SseStreamer({ signal: controller.signal });
  const clientOne = new SseStreamer();
  const clientTwo = new SseStreamer();
  const firstPromise = clientOne.take(original.getReadableStream());
  const secondPromise = clientTwo.take(original.getReadableStream());
  const firstReader = createReader(clientOne);
  const secondReader = createReader(clientTwo);

  await original.emit(chunks[0]);

  assertEquals(
    (await readChunk(firstReader, "frame 1 - client 1")).value,
    chunks[0],
  );
  assertEquals(
    (await readChunk(secondReader, "frame 1 - client 2")).value,
    chunks[0],
  );

  controller.abort(new Error("boom"));

  await assertRejects(() => firstReader.read(), Error, "boom");
  await assertRejects(() => secondReader.read(), Error, "boom");
  await Promise.all([firstPromise, secondPromise]);
});

Deno.test("should be able to emit on an stream that has an upstream", async () => {
  const chunks = ["id: 1\ndata: x\n\n", "id: 2\ndata: x\n\n"];
  const original = new SseStreamer();
  const client = new SseStreamer();
  const promise = client.take(original.getReadableStream());
  const reader = createReader(client);

  await original.emit(chunks[0]);
  await client.emit(chunks[1]);

  assertEquals((await readChunk(reader, "frame 1")).value, chunks[0]);
  assertEquals((await readChunk(reader, "frame 2")).value, chunks[1]);

  await original.close();
  await promise;
});
