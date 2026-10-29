import { SubscriberMultiTopic } from "./subscriber.ts";

export class MultiTopicUseCase {
  subscribers: Map<SubscriberMultiTopic, Record<string, boolean>> = new Map();

  protected _ctx: ConstructorParameters<typeof MultiTopicUseCase>[0];
  protected _env: ConstructorParameters<typeof MultiTopicUseCase>[1];

  constructor(
    ctx: Partial<Pick<DurableObjectState, "id">>,
    env: Pick<Env, "SINGLE_TOPIC_USE_CASE">,
  ) {
    this._ctx = ctx;
    this._env = env;
  }

  async fetch(request: Request) {
    return this.get(request);
  }

  async get(request: Request) {
    const subscriber = await SubscriberMultiTopic.from(request);

    let upstreamResponses: Array<{ match: string; response: Response }>;
    try {
      upstreamResponses = await Promise.all(
        subscriber.topics.map(async ({ match }) => {
          const singleTopicUrl = new URL(request.url);
          singleTopicUrl.searchParams.set("match", match);
          const id = this._env.SINGLE_TOPIC_USE_CASE.idFromName(match);
          return {
            match,
            response: await this._env.SINGLE_TOPIC_USE_CASE.get(id).fetch(
              new Request(singleTopicUrl, {
                headers: request.headers,
                method: request.method,
                signal: request.signal,
              }),
            ),
          };
        }),
      );
    } catch (_error) {
      return new Response("Bad Gateway", {
        status: 502,
        statusText: "Bad Gateway",
      });
    }

    const failed = upstreamResponses.find(({ response }) => !response.ok);
    if (failed) {
      return new Response(failed.response.body, {
        status: failed.response.status,
        statusText: failed.response.statusText,
      });
    }

    subscriber.closed.catch(() => {
      this.subscribers.delete(subscriber);
    });

    const subscriberState: Record<string, boolean> = Object.create(null);
    this.subscribers.set(subscriber, subscriberState);

    for (const { match, response } of upstreamResponses) {
      if (response.body) {
        subscriber.take(response.body);
        subscriberState[match] = true;
      }
    }

    return new Response(subscriber.getReadableStream(), {
      headers: {
        "cache-control": "no-cache, no-transform",
        "content-type": "text/event-stream",
        "connection": "keep-alive",
      },
    });
  }
}
