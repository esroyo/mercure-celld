import { SubscriberLike, type TopicLike } from "./types.ts";
import { SseUpdate } from "./sse_update.ts";
import { Publisher } from "./publisher.ts";
import { Subscriber } from "./subscriber.ts";
import { Topic } from "./topic.ts";

export class PatternTopicUseCase {
  #ctx: DurableObjectState;
  #subscribers = new Set<SubscriberLike>();

  constructor(ctx: DurableObjectState, _env: Env) {
    this.#ctx = ctx;
  }

  async fetch(request: Request) {
    if (request.method === "POST") {
      return this.post(request);
    }
    return this.get(request);
  }

  async get(request: Request) {
    const subscriber = await Subscriber.from(request);

    subscriber.closed.catch(() => {
      this.#subscribers.delete(subscriber);
    });

    this.#subscribers.add(subscriber);

    return new Response(subscriber.getReadableStream(), {
      headers: {
        "cache-control": "no-cache, no-transform",
        "content-type": "text/event-stream",
        "connection": "keep-alive",
      },
    });
  }

  async post(request: Request) {
    const publisher = await Publisher.from(request);
    for (const pubTopic of publisher.topics) {
      for (const subscriber of this.#subscribers) {
        for (const subTopic of subscriber.topics) {
          if (subTopic.test(pubTopic)) {
            subscriber.connect(topic, xxx);
          }
        }
      }
    }
  }
}
