import { monotonicUlid as _ulid } from "@std/ulid";
import type { PublisherLike, SseUpdateLike } from "./types.ts";
import { parsePostRequest } from "./utils.ts";

export class Publisher implements PublisherLike {
  id: PublisherLike["id"];
  body: PublisherLike["body"];

  constructor(
    { id, body }:
      & Partial<Pick<PublisherLike, "id">>
      & Required<Pick<PublisherLike, "body">> = { body: { topic: [] } },
    { ulid = _ulid }: { ulid?: () => string } = {},
  ) {
    this.id = id || ulid();
    this.body = body;
  }

  get topic() {
    return this.body.topic[0];
  }

  async isAuthorized(_update: SseUpdateLike) {
    // TODO
    return true;
  }

  static async from(request: Request) {
    const body = await parsePostRequest(request);
    if (!body) throw new Error("Bad Request");
    return new Publisher({ body });
  }
}
