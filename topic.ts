import type { SseUpdateLike, SseUpdateRow, TopicLike } from "./types.ts";
import { SseUpdate } from "./sse_update.ts";

type PartialSqlStorage = {
  exec<T extends Record<string, SqlStorageValue>>(
    query: string,
    ...bindings: any[]
  ): { toArray(): T[] };
};

export class Topic implements TopicLike {
  name: ConstructorParameters<typeof Topic>[0]["name"];

  protected _sql: ConstructorParameters<typeof Topic>[1]["sql"];

  constructor(
    { name }: Pick<TopicLike, "name">,
    { sql }: { sql: PartialSqlStorage },
  ) {
    this.name = name;
    this._sql = sql;
    this._sql.exec(`
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

  async addSseUpdate(update: SseUpdateLike) {
    this._sql.exec(
      "INSERT INTO sse_updates VALUES (?, ?, ?, ?, ?, ?)",
      update.eventData.length ? update.eventData.join("\n") : null,
      update.eventId,
      update.eventRetry,
      update.eventType,
      update.id,
      update.private,
    ).toArray();
  }

  async findSseUpdate(
    filter: { where?: { id: { gt: string } } | { event_id: string } } = {},
  ): Promise<SseUpdateLike[]> {
    const { where = { id: { gt: "" } } } = filter;
    const sqlArgs: [string, string] = "event_id" in where
      ? ["SELECT * FROM sse_updates WHERE event_id = ?", where.event_id]
      : ["SELECT * FROM sse_updates WHERE id > ? ORDER BY id ASC", where.id.gt];
    const rows = this._sql.exec<SseUpdateRow>(...sqlArgs).toArray();
    return Promise.all(
      rows.map((row) => SseUpdate.from({ ...row, topic: this.name })),
    );
  }
}
