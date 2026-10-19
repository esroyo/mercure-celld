import { monotonicUlid as _ulid } from "@std/ulid";
import type { SseUpdateLike, SubscriberLike } from "./types.ts";
import { parseGetRequest } from "./utils.ts";

export class Subscriber implements SubscriberLike {
  id: SubscriberLike["id"];
  lastEventId: SubscriberLike["lastEventId"];
  topic: SubscriberLike["topic"];

  #encoder = new TextEncoder();
  #stream = new TransformStream();
  #writer = this.#stream.writable.getWriter();

  constructor(
    { id, lastEventId, topic }:
      & Partial<Pick<SubscriberLike, "id" | "lastEventId">>
      & Required<Pick<SubscriberLike, "topic">> = { topic: "" },
    { ulid = _ulid }: { ulid?: () => string } = {},
  ) {
    this.id = id || ulid();
    this.lastEventId = lastEventId ?? null;
    this.topic = topic ?? "";
  }

  async isAuthorized(update: SseUpdateLike) {
    if (!update.private) {
      return true;
    }
    // TODO
    return false;
  }

  get closed() {
    return this.#writer.closed;
  }

  async send(text: string): Promise<void> {
    await this.#writer.write(this.#encoder.encode(text));
  }

  getReadableStream(): ReadableStream {
    return this.#stream.readable;
  }

  static async from(request: Request) {
    const { lastEventId, topics } = parseGetRequest(request);
    return new Subscriber({
      lastEventId: lastEventId === "earliest" ? "" : lastEventId,
      topic: topics[0].match,
    });
  }
}
