import { assertEquals } from "jsr:@std/assert/equals";
import { Publisher } from "./publisher.ts";

Deno.test("should take the topic value from the request field `topic` at index 0", async (_t) => {
  const subscriber = await Publisher.from(
    new Request("http://localhost", {
      body: new URLSearchParams([["topic", "a"], ["topic", "b"]]),
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
  );
  assertEquals(subscriber.topic, "a");
});
