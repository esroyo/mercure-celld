import { assertEquals, assertRejects } from "@std/assert";
import {
  Subscriber,
  SubscriberMultiTopic,
  SubscriberSingleTopic,
} from "./subscriber.ts";

Deno.test("[Subscriber] should keep the full topics list and expose the first as topic", async () => {
  const subscriber = await Subscriber.from(
    new Request("http://localhost/?match=a&match=b"),
  );
  assertEquals(subscriber.topics, [{ match: "a" }, { match: "b" }]);
  assertEquals(subscriber.topic, "a");
});

Deno.test.each([
  ["multiple exact topics", "/?match=a&match=b"],
  ["duped exact topic", "/?match=a&match=a"],
  ["non-exact topic", "/?match_urlpattern=foo*"],
  ["exact topic mixed with non-exact topic", "/?match=a&match_urlpattern=foo*"],
])(
  "[SubscriberSingleTopic] should not construct when given %s",
  async (_desc, params) => {
    await assertRejects(() =>
      SubscriberSingleTopic.from(
        new Request(new URL(params, "http://localhost")),
      )
    );
  },
);

Deno.test("[SubscriberSingleTopic] should construct when given an exact topic", async () => {
  const _subscriber = await SubscriberSingleTopic.from(
    new Request("http://localhost/?match=a"),
  );
});

Deno.test.each([
  ["single exact topic", "/?match=a"],
  ["duped exact topic", "/?match=a&match=a"],
  ["duped exact topics", "/?match=a&match=b&match=a"],
  ["non-exact topic", "/?match_urlpattern=foo*"],
  [
    "exact topic mixed with non-exact topic",
    "/?match=a&match=b&match_urlpattern=foo*",
  ],
])(
  "[SubscriberMultiTopic] should not construct when given %s",
  async (_desc, params) => {
    await assertRejects(() =>
      SubscriberMultiTopic.from(
        new Request(new URL(params, "http://localhost")),
      )
    );
  },
);

Deno.test.each([
  ["2 exact topics", "/?match=a&match=b"],
  ["+2 exact topics", "/?match=a&match=b&match=c"],
])(
  "[SubscriberMultiTopic] should construct when given %s",
  async (_desc, params) => {
    const _subscriber = await SubscriberMultiTopic.from(
      new Request(new URL(params, "http://localhost")),
    );
  },
);
