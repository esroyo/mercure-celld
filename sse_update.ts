import { monotonicUlid as _ulid } from "@std/ulid";
import type { SseUpdateLike, SseUpdatePost, SseUpdateRow } from "./types.ts";

export class SseUpdate implements SseUpdateLike {
  eventData: SseUpdateLike["eventData"];
  eventId: SseUpdateLike["eventId"];
  eventType: SseUpdateLike["eventType"];
  eventRetry: SseUpdateLike["eventRetry"];
  id: SseUpdateLike["id"];
  private: SseUpdateLike["private"];
  topic: SseUpdateLike["topic"];

  constructor(
    {
      eventData,
      eventId,
      eventRetry,
      eventType,
      id,
      private: isPrivate,
      topic,
    }:
      & Partial<Omit<SseUpdateLike, "topic">>
      & Pick<SseUpdateLike, "topic">,
    { ulid = _ulid }: { ulid?: () => string } = {},
  ) {
    this.id = id || ulid();
    this.topic = topic;
    this.eventData = eventData && !Array.isArray(eventData)
      ? [eventData]
      : eventData ?? [];
    this.eventId = eventId ?? null;
    this.eventRetry = eventRetry ?? null;
    this.eventType = eventType ?? null;
    this.private = isPrivate ?? 0;
  }

  toString() {
    const textParts = [];
    if (this.eventId !== null) {
      textParts.push(`id: ${this.eventId}`);
    } else {
      textParts.push(`id: #${this.id}`);
    }
    for (const data of this.eventData) {
      textParts.push(`data: ${data}`);
    }
    if (this.eventType) {
      textParts.push(`event: ${this.eventType}`);
    }
    if (this.eventRetry) {
      textParts.push(`retry: ${this.eventRetry}`);
    }
    textParts.push("", "");
    return textParts.join("\n");
  }

  static async from(
    input:
      | SseUpdatePost
      | SseUpdateLike
      | (SseUpdateRow & Pick<SseUpdateLike, "topic">),
  ) {
    // SseUpdate instance
    if ("eventId" in input) {
      return new SseUpdate(input);
    }
    // SseUpdate row
    if ("event_id" in input) {
      return new SseUpdate({
        id: input.id,
        eventData: input.event_data?.split("\n"),
        eventId: input.event_id,
        eventRetry: Number(input.event_retry) || undefined,
        eventType: input.event_type,
        private: input.private,
        topic: input.topic,
      });
    }
    // SseUpdate POST body
    return new SseUpdate({
      eventData: input.data,
      eventId: input.id,
      eventRetry: Number(input.retry) || undefined,
      eventType: input.type,
      private: "private" in input && !!input.private ? 1 : 0,
      topic: input.topic[0],
    });
  }
}
