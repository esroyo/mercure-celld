import { parseGetRequest, parsePostRequest } from "./utils.ts";
export { SingleTopicUseCase } from "./single_topic_use_case.ts";
export { MultiTopicUseCase } from "./multi_topic_use_case.ts";

export default {
  async fetch(request: Request, env: Env) {
    if (request.method === "GET") {
      const { topics } = parseGetRequest(request);
      if (!topics.length) {
        return new Response("Not Found", { status: 404 });
      } else if (topics.length === 1) {
        const id = env.SINGLE_TOPIC_USE_CASE.idFromName(topics[0].match);
        return env.SINGLE_TOPIC_USE_CASE.get(id).fetch(request);
      } else {
        const id = env.MULTI_TOPIC_USE_CASE.idFromName("multi-topic");
        return env.MULTI_TOPIC_USE_CASE.get(id).fetch(request);
      }
    }
    if (request.method === "POST") {
      const body = await parsePostRequest(request);
      const responses = await Promise.all(body.topics.map(async ({ match }) => {
        const id = env.SINGLE_TOPIC_USE_CASE.idFromName(match);
        const clone = new URLSearchParams(
          body.data.map((value) => ["data", value]),
        );
        clone.append("topic", match);
        for (const key of ["id", "private", "retry", "type"]) {
          const value = body[key];
          if (value) clone.append(key, value);
        }
        return await env.SINGLE_TOPIC_USE_CASE.get(id).fetch(
          new Request(request.url, {
            body: clone,
            headers: request.headers,
            method: request.method,
            signal: request.signal,
          }),
        );
      }));
      const failed = responses.find((response) => !response.ok);
      if (failed) {
        return new Response(failed.response.body, {
          status: failed.response.status,
          statusText: failed.response.statusText,
        });
      }
      return new Response(null, { status: 200 });
    }
  },
};
