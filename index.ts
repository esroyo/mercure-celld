import { parseGetRequest, parsePostRequest } from "./utils.ts";
export { SingleTopicUseCase } from "./single_topic_use_case.ts";

export default {
  async fetch(request: Request, env: Env) {
    if (request.method === "GET") {
      const { topics } = parseGetRequest(request);
      if (topics.length !== 1) {
        return new Response("Not Found", { status: 404 });
      }
      const id = env.SINGLE_TOPIC_USE_CASE.idFromName(topics[0].match);
      return env.SINGLE_TOPIC_USE_CASE.get(id).fetch(request);
    }
    if (request.method === "POST") {
      const body = await parsePostRequest(request);
      for (const topic of body.topic) {
        const id = env.SINGLE_TOPIC_USE_CASE.idFromName(topic);
        const clone = new URLSearchParams(
          body.data.map((value) => ["data", value]),
        );
        clone.append("topic", topic);
        for (const key of ["id", "private", "retry", "type"]) {
          const value = body[key];
          if (value) clone.append(key, value);
        }
        return env.SINGLE_TOPIC_USE_CASE.get(id).fetch(
          new Request(request.url, {
            body: clone,
            headers: request.headers,
            method: request.method,
            signal: request.signal,
          }),
        );
      }
    }
  },
};
