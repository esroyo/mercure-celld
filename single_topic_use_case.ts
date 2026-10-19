import type { TopicLike } from "./types.ts";
import { SseUpdate } from "./sse_update.ts";
import { Publisher } from "./publisher.ts";
import { Subscriber } from "./subscriber.ts";
import { Topic } from "./topic.ts";

export class SingleTopicUseCase {
  #ctx: DurableObjectState;
  #topic: TopicLike;

  constructor(ctx: DurableObjectState, _env: Env) {
    this.#ctx = ctx;
    this.#topic = new Topic({ name: this.#ctx.id.name }, {
      sql: ctx.storage.sql,
    });
  }

  async fetch(request: Request) {
    if (request.method === "POST") {
      return this.post(request);
    }
    return this.get(request);
  }

  async get(request: Request) {
    const subscriber = await Subscriber.from(request);
    this.#topic.addSubscriber(subscriber);
    return new Response(subscriber.getReadableStream(), {
      headers: {
        "cache-control": "no-cache, no-transform",
        "content-type": "text/event-stream",
        "connection": "keep-alive",
      },
    });
  }

  async post(request: Request) {
    try {
      const publisher = await Publisher.from(request);
      const update = await SseUpdate.from(publisher.body);
      if (!publisher.isAuthorized(update)) {
        return new Response("Unauthorized", { status: 401 });
      }
      await this.#topic.addSseUpdate(update);
      return new Response(null, { status: 204 });
    } catch (error) {
      const message = Error.isError(error) ? error.message : "Bad Request";
      return new Response(message, { status: 400 });
    }
  }
}
