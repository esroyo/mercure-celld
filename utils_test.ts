import { assertEquals } from "@std/assert";
import { parseGetRequest, parsePostRequest } from "./utils.ts";

Deno.test("[parseGetRequest] should default the topics value to empty array", async (_t) => {
  const request = new Request("http://localhost/");
  assertEquals(parseGetRequest(request).topics, []);
});

Deno.test("[parseGetRequest] should take the topics value from topic param", async (_t) => {
  const request = new Request("http://localhost/?topic=a");
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }]);
});

Deno.test("[parseGetRequest] should take the topics value from topic params", async (_t) => {
  const request = new Request("http://localhost/?topic=a&topic=b");
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }, {
    match: "b",
  }]);
});

Deno.test("[parseGetRequest] should take the topics value from topic array-param", async (_t) => {
  const request = new Request("http://localhost/?topic[]=a&topic[]=b");
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }, {
    match: "b",
  }]);
});

Deno.test("[parseGetRequest] should take the topics value from match param", async (_t) => {
  const request = new Request("http://localhost/?match=a");
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }]);
});

Deno.test("[parseGetRequest] should take the topics value from match params", async (_t) => {
  const request = new Request("http://localhost/?match=a&match=b");
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }, {
    match: "b",
  }]);
});

Deno.test("[parseGetRequest] should take the topics value from match array-param", async (_t) => {
  const request = new Request("http://localhost/?match[]=a&match[]=b");
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }, {
    match: "b",
  }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_exact param", async (_t) => {
  const request = new Request("http://localhost/?match_exact=a");
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_exact params", async (_t) => {
  const request = new Request("http://localhost/?match_exact=a&match_exact=b");
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }, {
    match: "b",
  }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_exact array-param", async (_t) => {
  const request = new Request(
    "http://localhost/?match_exact[]=a&match_exact[]=b",
  );
  assertEquals(parseGetRequest(request).topics, [{ match: "a" }, {
    match: "b",
  }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_urlpattern param", async (_t) => {
  const request = new Request("http://localhost/?match_urlpattern=%2F%3Aid");
  assertEquals(parseGetRequest(request).topics, [{
    match: "/:id",
    matchType: "urlpattern",
  }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_urlpattern params", async (_t) => {
  const request = new Request(
    "http://localhost/?match_urlpattern=%2F%3Aid&match_urlpattern=%2Fbooks%2F*",
  );
  assertEquals(parseGetRequest(request).topics, [{
    match: "/:id",
    matchType: "urlpattern",
  }, { match: "/books/*", matchType: "urlpattern" }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_urlpattern array-param", async (_t) => {
  const request = new Request(
    "http://localhost/?match_urlpattern[]=%2F%3Aid&match_urlpattern[]=%2Fbooks%2F*",
  );
  assertEquals(parseGetRequest(request).topics, [{
    match: "/:id",
    matchType: "urlpattern",
  }, { match: "/books/*", matchType: "urlpattern" }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_urlpattern_exact param", async (_t) => {
  const request = new Request("http://localhost/?match_urlpattern=%2F%3Aid");
  assertEquals(parseGetRequest(request).topics, [{
    match: "/:id",
    matchType: "urlpattern",
  }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_urlpattern_exact params", async (_t) => {
  const request = new Request(
    "http://localhost/?match_urlpattern=%2F%3Aid&match_urlpattern=%2Fbooks%2F*",
  );
  assertEquals(parseGetRequest(request).topics, [{
    match: "/:id",
    matchType: "urlpattern",
  }, { match: "/books/*", matchType: "urlpattern" }]);
});

Deno.test("[parseGetRequest] should take the topics value from match_urlpattern_exact array-param", async (_t) => {
  const request = new Request(
    "http://localhost/?match_urlpattern[]=%2F%3Aid&match_urlpattern[]=%2Fbooks%2F*",
  );
  assertEquals(parseGetRequest(request).topics, [{
    match: "/:id",
    matchType: "urlpattern",
  }, { match: "/books/*", matchType: "urlpattern" }]);
});

Deno.test("[parseGetRequest] should default the lastEventId value to null", async (_t) => {
  const request = new Request("http://localhost/");
  assertEquals(parseGetRequest(request).lastEventId, null);
});

Deno.test("[parseGetRequest] should take the lastEventId value from header Last-Event-Id (priority 3)", async (_t) => {
  const request = new Request("http://localhost/", {
    headers: { "Last-Event-Id": "x" },
  });
  assertEquals(parseGetRequest(request).lastEventId, "x");
});

Deno.test("[parseGetRequest] should take the lastEventId value from param lastEventID param (priority 2)", async (_t) => {
  const request = new Request("http://localhost/?lastEventID=y", {
    headers: { "Last-Event-Id": "x" },
  });
  assertEquals(parseGetRequest(request).lastEventId, "y");
});

Deno.test("[parseGetRequest] should take the lastEventId value from param lastEventId param (priority 1)", async (_t) => {
  const request = new Request("http://localhost/?lastEventID=y&lastEventId=z", {
    headers: { "Last-Event-Id": "z" },
  });
  assertEquals(parseGetRequest(request).lastEventId, "z");
});

Deno.test("[parseGetRequest] should take the lastEventId value from param last_event_id param (priority 0)", async (_t) => {
  const request = new Request(
    "http://localhost/?last_event_id=zz&lastEventID=y&lastEventId=z",
    {
      headers: { "Last-Event-Id": "zz" },
    },
  );
  assertEquals(parseGetRequest(request).lastEventId, "zz");
});

Deno.test("[parsePostRequest] should take the topics value from topic body field", async (_t) => {
  const request = new Request("http://localhost/", {
    body: new URLSearchParams([["topic", "a"]]),
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    method: "POST",
  });
  assertEquals((await parsePostRequest(request)).topics, [{ match: "a" }]);
});

Deno.test("[parsePostRequest] should take the topics value from topic body fields", async (_t) => {
  const request = new Request("http://localhost/", {
    body: new URLSearchParams([["topic", "a"], ["topic", "b"]]),
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    method: "POST",
  });
  assertEquals((await parsePostRequest(request)).topics, [{ match: "a" }, {
    match: "b",
  }]);
});
