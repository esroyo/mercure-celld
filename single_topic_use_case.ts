import type {
  StreamerLike,
  SubscriberStreamerLike,
  TopicLike,
} from "./types.ts";
import { SseUpdate } from "./sse_update.ts";
import { Publisher } from "./publisher.ts";
import { SubscriberSingleTopic } from "./subscriber.ts";
import { Topic } from "./topic.ts";
import { SseStreamer } from "./sse_streamer.ts";

export class SingleTopicUseCase {
  channel: Record<"private" | "public", StreamerLike> = {
    private: new SseStreamer(),
    public: new SseStreamer(),
  };
  subscribers: Set<SubscriberStreamerLike> = new Set();
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
    const bodyStream = await this.addSubscriber(subscriber, {
      signal: request.signal,
    });
    return new Response(bodyStream, {
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
      const isAuthorized = await publisher.isAuthorized({
        topic: this.topic.name,
      });
      if (!isAuthorized) {
        return new Response("Unauthorized", { status: 401 });
      }
      await this.topic.addSseUpdate(update);
      const channelName = update.private ? "private" : "public";
      await this.channel[channelName].emit(update.toString());
      return new Response(null, { status: 204 });
    } catch (error) {
      const message = Error.isError(error) ? error.message : "Bad Request";
      return new Response(message, { status: 400 });
    }
  }

  async addSubscriber(
    subscriber: SubscriberStreamerLike,
    options: { signal?: AbortSignal } = {},
  ) {
    const bodyStream = subscriber.getReadableStream();
    options.signal?.addEventListener("abort", () => {
      subscriber.abort(options.signal.reason);
    }, { once: true });
    const isAuthorized = await subscriber.isAuthorized({
      topic: this.topic.name,
    });
    if (isAuthorized) {
      subscriber.take(this.channel.private.getReadableStream());
    }
    subscriber.take(this.channel.public.getReadableStream());

    this.subscribers.add(subscriber);

    subscriber.closed.catch(() => {
      this.subscribers.delete(subscriber);
    });

    if (subscriber.lastEventId !== null) {
      let [isEventId, effectiveId] =
        subscriber.lastEventId === "" || subscriber.lastEventId.startsWith("#")
          ? [false, subscriber.lastEventId.slice(1)]
          : [true, subscriber.lastEventId];
      if (isEventId) {
        const [foundUpdate] = await this.topic.findSseUpdate({
          where: { event_id: effectiveId },
        });
        effectiveId = foundUpdate?.id;
      }
      if (effectiveId !== undefined) {
        const missedUpdates = await this.topic.findSseUpdate({
          where: { id: { gt: effectiveId } },
        });
        for (const update of missedUpdates) {
          if (!update.private || isAuthorized) {
            subscriber.emit(update.toString());
          }
        }
      }
    }

    return bodyStream;
  }
}
