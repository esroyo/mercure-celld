import type { TopicLike } from "./types.ts";
import { SseUpdate } from "./sse_update.ts";
import { Publisher } from "./publisher.ts";
import { SubscriberSingleTopic } from "./subscriber.ts";
import { Topic } from "./topic.ts";

export class SingleTopicUseCase {
  topic: TopicLike;

  protected _ctx: ConstructorParameters<typeof SingleTopicUseCase>[0];
  protected _env: ConstructorParameters<typeof SingleTopicUseCase>[1];

  constructor(
    ctx: Pick<DurableObjectState, "id" | "storage">,
    env: Partial<Pick<Env, "SINGLE_TOPIC_USE_CASE">>,
  ) {
    this._ctx = ctx;
    this._env = env;
    this.topic = new Topic({ name: this._ctx.id.name }, {
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
    const subscriber = await SubscriberSingleTopic.from(request);
    this.topic.addSubscriber(subscriber);
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
      if (await publisher.isAuthorized(update)) {
        await this.topic.addSseUpdate(update);
        return new Response(null, { status: 204 });
      }
      return new Response("Unauthorized", { status: 401 });
    } catch (error) {
      const message = Error.isError(error) ? error.message : "Bad Request";
      return new Response(message, { status: 400 });
    }
  }
}
