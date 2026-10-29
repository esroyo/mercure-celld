import { monotonicUlid as _ulid } from "@jsr/std__ulid";
import type {
  Match,
  StreamerLike,
  SubscriberLike,
  SubscriberStreamerLike,
} from "./types.ts";
import { parseGetRequest } from "./utils.ts";
import { SseStreamer } from "./sse_streamer.ts";

export class Subscriber implements SubscriberStreamerLike {
  id: string;
  lastEventId: string;
  topics: Match[];
  channel: StreamerLike = new SseStreamer();

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

  async isAuthorized(options: { topic: string }) {
    // TODO
    return false;
  }

  get abort() {
    return this.channel.abort.bind(this.channel);
  }

  get close() {
    return this.channel.close.bind(this.channel);
  }

  get closed() {
    return this.channel.closed;
  }

  get getReadableStream() {
    return this.channel.getReadableStream.bind(this.channel);
  }

  get emit() {
    return this.channel.emit.bind(this.channel);
  }

  get take() {
    return this.channel.take.bind(this.channel);
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

export class SubscriberSingleTopic extends Subscriber {
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
}

export class SubscriberMultiTopic extends Subscriber {
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
}
