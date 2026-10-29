export type SseUpdatePost = {
  /**
   * Identifier of the updated topic. MAY appear more than once: the
   * first occurrence is the canonical topic, any others are
   * alternate topics.
   */
  topics: Match[];

  /**
   * Payload of the update. Anything you want: JSON, HTML, JSON Patch, plain text.
   */
  data?: string[];

  /**
   * If present, the update is private. The hub delivers it only to
   * subscribers authorized for the topic.
   *
   * Without the private field, an update is public: the hub sends
   * it to every subscriber whose matchers hit, regardless of whether
   * they presented a token.
   *
   * With private=on (the value can be anything; on is the convention),
   * the update is private: a subscriber receives it only if its token
   * grants subscribe on at least one of the update's topics.
   */
  private?: string;

  /**
   * Custom event ID. Must not start with # or equal the reserved value
   * earliest. The hub assigns one if you don't.
   */
  id?: string;

  /**
   * Custom SSE event type. Defaults to message. mercure is reserved for
   * hub-generated events and is rejected with a 400.
   */
  type?: string;

  /**
   * Reconnection time hint, in milliseconds.
   */
  retry?: string;
};

export interface SseUpdateLike {
  /**
   * The data field for the message. When the EventSource receives
   * multiple consecutive lines that begin with data:, it
   * concatenates them, inserting a newline character between each
   * one. Trailing newlines are removed.
   */
  eventData: string[];

  /**
   * The event ID to set the EventSource object's last event ID value.
   */
  eventId: string;

  /**
   * A string identifying the type of event described.
   * If this is specified, an event will be dispatched on the
   * browser to the listener for the specified event name; the
   * website source code should use addEventListener() to listen
   * for named events. The onmessage handler is called if no event
   * name is specified for a message.
   */
  eventType: string | null;

  /**
   * The reconnection time. If the connection to the server is lost,
   * the browser will wait for the specified time before attempting
   * to reconnect. This must be an integer, specifying the
   * reconnection time in milliseconds. If a non-integer value is
   * specified, the field is ignored.
   */
  eventRetry: number | null;

  /**
   * The internal ULID.
   */
  id: string;

  /**
   * The public ID of the updated Topic. MAY appear more than once: the
   * first occurrence is the canonical topic, any others are
   * alternate topics.
   */
  topic: string;

  /**
   * If present, the message is private. The hub delivers it only to subscribers authorized for the topic.
   */
  private: 0 | 1;
}

export type SseUpdateRow = {
  event_id: string | null;
  event_data: string | null;
  event_type: string | null;
  event_retry: number | null;
  id: string;
  private: 0 | 1;
};

/**
 * Represents a client for a specific topic.
 */
export interface ClientLike {
  /**
   * The internal ULID for this client.
   */
  id: string;

  /**
   * The public ID of the target Topic.
   */
  topic: string;
  topics: Match[];

  isAuthorized(options: { topic: string }): Promise<boolean>;
}

/**
 * Represents a subscriber's state and details for a specific topic.
 */
export interface SubscriberLike extends ClientLike {
  /**
   * The event ID of the last update received by the subscriber.
   *
   * Used on reconnection as the marker to resume pending messages.
   *
   * - `""` (empty string): Receive all messages since the start of the topic.
   * - `null`: Receive all messages published from the exact moment the subscription was created.
   */
  lastEventId: string | null;
}

export interface SubscriberStreamerLike extends SubscriberLike, StreamerLike {}

/**
 * Represents a publisher's state and details for a specific topic.
 */
export interface PublisherLike extends ClientLike {
  body: SseUpdatePost;
}

type SseUpdateFilterQuery = {
  where?: { id: { gt: string } } | { event_id: string };
};

/**
 * Represents a topic state.
 */
export interface TopicLike {
  /**
   * The public ID of the Topic.
   */
  name: string;
  addSseUpdate(update: SseUpdateLike): Promise<void>;
  findSseUpdate(filter?: SseUpdateFilterQuery): Promise<SseUpdateLike[]>;
}

export type MatchType = "exact" | "urlpattern";
export type Match = {
  match: string;
  matchType?: MatchType;
};

export interface StreamerLike {
  abort(reason?: unknown): Promise<void>;
  close: () => Promise<void>;
  closed: Promise<void>;
  getReadableStream(
    options?: { preventCancel?: boolean },
  ): ReadableStream<Uint8Array>;
  take(
    upstream: ReadableStream,
    options?: { preventClose?: boolean },
  ): Promise<void>;
  emit(text: string): Promise<void>;
}
