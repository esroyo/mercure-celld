import { Match, SseUpdatePost } from "./types.ts";

export function parseGetRequest(
  request: Request,
): { lastEventId: string | "earliest" | null; topics: Match[] } {
  const url = new URL(request.url);
  const lastEventId = url.searchParams.get("last_event_id") ??
    url.searchParams.get("lastEventId") ??
    url.searchParams.get("lastEventID") ??
    request.headers.get("last-event-id");
  const topics: Match[] = [];
  for (
    const param of [
      "topic",
      "match",
      "match_exact",
      "topic[]",
      "match[]",
      "match_exact[]",
    ]
  ) {
    for (const value of url.searchParams.getAll(param)) {
      const match = value.trim();
      if (match) topics.push({ match });
    }
  }
  for (const param of ["match_urlpattern", "match_urlpattern[]"]) {
    const matchType = "urlpattern";
    for (const value of url.searchParams.getAll(param)) {
      const match = value.trim();
      if (match) topics.push({ match, matchType });
    }
  }
  return {
    lastEventId,
    topics,
  };
}

export async function parsePostRequest(
  request: Request,
): Promise<SseUpdatePost | null> {
  if (
    request.headers.get("content-type") !== "application/x-www-form-urlencoded"
  ) return null;
  const formData = await request.formData();
  return {
    id: formData.get("id"),
    data: formData.getAll("data"),
    private: formData.get("private"),
    retry: formData.get("retry"),
    topics: formData.getAll("topic").map((t) => ({ match: t as string })),
    type: formData.get("type"),
  } as SseUpdatePost;
}
