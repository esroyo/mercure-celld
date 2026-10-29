import type { StreamerLike } from "./types.ts";

export class SseStreamer implements StreamerLike {
  protected _aborted = false;
  protected _encoder = new TextEncoder();
  protected _stream = new TransformStream();
  protected _writer = this._stream.writable.getWriter();
  protected _upstreams: Set<ReadableStream> = new Set();
  protected _downstreams = new Set<
    ReadableStreamDefaultController<Uint8Array>
  >();
  protected _terminal?: { done: true } | { error: unknown };

  constructor({ signal }: { signal?: AbortSignal } = {}) {
    signal?.addEventListener("abort", () => {
      void this.abort(signal.reason);
    }, { once: true });
    void this._pump();
  }

  close(): Promise<void> {
    return this._writer.close().catch(() => {});
  }

  abort(reason?: unknown): Promise<void> {
    return this._writer.abort(reason).catch(() => {});
  }

  get closed() {
    return this._writer.closed;
  }

  /**
   * "From now" contract: a returned stream only carries frames emitted
   * after its creation. Anything emitted while no consumer is attached
   * is dropped by the pump and never replayed.
   */
  getReadableStream(
    options: { preventCancel?: boolean } = {},
  ): ReadableStream<Uint8Array> {
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    const stream = new ReadableStream<Uint8Array>({
      start: (c) => {
        controller = c;
      },
      cancel: (reason: unknown) => {
        this._downstreams.delete(controller);
        if (options.preventCancel) return;
        if (this._downstreams.size === 0) {
          void this.abort(reason);
        }
      },
    });
    this._downstreams.add(controller);
    const terminal = this._terminal;
    if (!terminal) return stream;
    if ("done" in terminal) {
      controller.close();
    } else {
      controller.error(terminal.error);
    }
    return stream;
  }

  protected async _pump() {
    const reader = this._stream.readable.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        this._downstreams.forEach((downstream) => downstream.enqueue(value));
      }
      this._terminal = { done: true };
      this._downstreams.forEach((downstream) => downstream.close());
    } catch (error) {
      this._terminal = { error };
      this._downstreams.forEach((downstream) => downstream.error(error));
    }
  }

  async emit(text: string): Promise<void> {
    await this._writer.write(this._encoder.encode(text));
  }

  async take(
    upstream: ReadableStream,
    options: { preventClose?: boolean } = {},
  ) {
    if (this._upstreams.has(upstream)) return;
    this._upstreams.add(upstream);
    const controller = new AbortController();
    this.closed.catch(() => controller.abort());
    const textUpstream = upstream.pipeThrough(new TextDecoderStream(), {
      signal: controller.signal,
    });
    let buffer = "";
    try {
      for await (const chunk of textUpstream) {
        buffer += chunk;
        const messages = buffer.split("\n\n");
        buffer = messages.pop();
        for (const msg of messages) {
          await this._writer.write(this._encoder.encode(msg + "\n\n"));
        }
      }
      if (buffer !== "") {
        await this._writer.write(this._encoder.encode(buffer));
      }
    } catch (error) {
      this._aborted = true;
      await this._writer.abort(error);
    } finally {
      this._upstreams.delete(upstream);
      if (
        !options.preventClose && !this._aborted &&
        this._upstreams.size === 0
      ) {
        await this._writer.close().catch(() => {});
      }
    }
  }
}
