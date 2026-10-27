import { monotonicUlid as _ulid } from "@jsr/std__ulid";
import type {
  Match,
  SseUpdateLike,
  SubscriberLike,
  SubscriberStreamLike,
  SubscriberStreamMultiTopicLike,
  SubscriberStreamSingleTopicLike,
} from "./types.ts";
import { parseGetRequest } from "./utils.ts";

export class Subscriber implements SubscriberStreamLike {
  id: string;
  lastEventId: string;
  topics: Match[];

  protected _encoder = new TextEncoder();
  protected _stream = new TransformStream();
  protected _writer = this._stream.writable.getWriter();

  constructor(
    { id, lastEventId, topics }:
      & Partial<Pick<SubscriberLike, "id" | "lastEventId">>
      & Required<Pick<SubscriberLike, "topics">> = { topics: [] },
    { ulid = _ulid }: { ulid?: () => string } = {},
  ) {
    this.id = id || ulid();
    this.lastEventId = lastEventId ?? null;
    this.topics = topics ?? [];
  }

  get topic() {
    return this.topics[0]?.match;
  }

  async isAuthorized(update: SseUpdateLike) {
    if (!update.private) {
      return true;
    }
    // TODO
    return false;
  }

  get closed() {
    return this._writer.closed;
  }

  getReadableStream(): ReadableStream {
    return this._stream.readable;
  }

  static async from<T>(
    this: new (...args: any[]) => T,
    request: Request,
  ): Promise<T> {
    const { lastEventId, topics } = parseGetRequest(request);
    return new this({
      lastEventId: lastEventId === "earliest" ? "" : lastEventId,
      topics,
    });
  }
}

export class SubscriberSingleTopic extends Subscriber
  implements SubscriberStreamSingleTopicLike {
  constructor(...args) {
    super(...args);
    const exact = new Set(
      this.topics.filter((t) => t.matchType !== "urlpattern").map((t) =>
        t.match
      ),
    );
    if (exact.size !== this.topics.length || this.topics.length !== 1) {
      throw new Error("SubscriberSingleTopic requires 1 exact topic");
    }
  }

  async send(text: string): Promise<void> {
    await this._writer.write(this._encoder.encode(text));
  }
}

export class SubscriberMultiTopic extends Subscriber
  implements SubscriberStreamMultiTopicLike {
  declare topics: Array<Match & { isConnected: boolean }>;
  protected _aborted = false;

  constructor(...args) {
    super(...args);
    const exact = new Set(
      this.topics.filter((t) => t.matchType !== "urlpattern").map((t) =>
        t.match
      ),
    );
    if (exact.size !== this.topics.length || this.topics.length < 2) {
      throw new Error("SubscriberMultiTopic requires 2 or more exact topics");
    }
  }

  async connect(topic: string, readableUpstream: ReadableStream) {
    const found = this.topics.find(({ match }) => match === topic);
    if (!found) {
      throw new Error(`SubscriberMultiTopic unknown topic: ${topic}`);
    }
    if (found.isConnected) {
      return;
    }

    found.isConnected = true;

    const upstreamController = new AbortController();
    this.closed.catch(() => upstreamController.abort());
    const textUpstream = readableUpstream.pipeThrough(new TextDecoderStream(), {
      signal: upstreamController.signal,
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
      const found = this.topics.find(({ match }) => match === topic);
      if (found) {
        found.isConnected = false;
      }
      const upConnections = this.topics.filter((t) => t.isConnected);
      if (!this._aborted && upConnections.length === 0) {
        await this._writer.close().catch(() => {});
      }
    }
  }
}
