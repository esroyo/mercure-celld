import type {
  SseUpdateLike,
  SseUpdateRow,
  SubscriberLike,
  TopicLike,
} from "./types.ts";
import { SseUpdate } from "./sse_update.ts";

type CtorParams = ConstructorParameters<typeof Topic>;

export class Topic implements TopicLike {
  name: CtorParams[0]["name"];

  #sql: CtorParams[1]["sql"];
  #subscribers: Set<SubscriberLike>;

  constructor(
    { name }: Pick<TopicLike, "name">,
    { sql }: { sql: Pick<SqlStorage, "exec"> },
  ) {
    this.name = name;
    this.#subscribers = new Set();
    this.#sql = sql;
    this.#sql.exec(`
        CREATE TABLE IF NOT EXISTS sse_updates (
          event_data TEXT,
          event_id TEXT UNIQUE,
          event_retry INTEGER,
          event_type TEXT,
          id TEXT PRIMARY KEY,
          private INTEGER NOT NULL DEFAULT 0 CHECK (private IN (0, 1))
        )
      `);
  }

  async addSubscriber(subscriber: SubscriberLike) {
    this.#subscribers.add(subscriber);

    subscriber.closed.catch(() => {
      this.#subscribers.delete(subscriber);
    });

    if (subscriber.lastEventId !== null) {
      let [isEventId, effectiveId] =
        subscriber.lastEventId === "" || subscriber.lastEventId.startsWith("#")
          ? [false, subscriber.lastEventId.slice(1)]
          : [true, subscriber.lastEventId];
      if (isEventId) {
        const [foundUpdate] = await this.findSseUpdate({
          where: { event_id: effectiveId },
        });
        effectiveId = foundUpdate?.id;
      }
      if (effectiveId !== undefined) {
        const missedUpdates = await this.findSseUpdate({
          where: { id: { gt: effectiveId } },
        });
        for (const update of missedUpdates) {
          subscriber.send(update.toString());
        }
      }
    }
  }

  async addSseUpdate(update: SseUpdateLike) {
    this.#sql.exec(
      "INSERT INTO sse_updates VALUES (?, ?, ?, ?, ?, ?)",
      update.eventData.join("\n"),
      update.eventId,
      update.eventRetry,
      update.eventType,
      update.id,
      update.private,
    ).toArray();
    const text = update.toString();
    for (const subscriber of this.#subscribers) {
      if (subscriber.isAuthorized(update)) {
        subscriber.send(text);
      }
    }
  }

  async findSseUpdate(
    filter: { where?: { id: { gt: string } } | { event_id: string } } = {},
  ): Promise<SseUpdateLike[]> {
    const { where = { id: { gt: "" } } } = filter;
    const sqlArgs: [string, string] = "event_id" in where
      ? ["SELECT * FROM sse_updates WHERE event_id = ?", where.event_id]
      : ["SELECT * FROM sse_updates WHERE id > ? ORDER BY id ASC", where.id.gt];
    const rows = this.#sql.exec<SseUpdateRow>(...sqlArgs).toArray();
    return Promise.all(
      rows.map((row) => SseUpdate.from({ ...row, topic: this.name })),
    );
  }
}
