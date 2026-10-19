const aggr = new TransformStream();
const writer = aggr.writable.getWriter();
let activeStreams = 0;

// You can call this function at any time with a new fetch Response
async function addDynamicStream(response) {
  activeStreams++;
  const textStream = response.body.pipeThrough(new TextDecoderStream());
  let buffer = "";

  try {
    for await (const chunk of textStream) {
      buffer += chunk;
      const messages = buffer.split("\n\n");
      buffer = messages.pop();

      for (const msg of messages) {
        // Await handles backpressure safely across all dynamic streams
        await writer.write(new TextEncoder().encode(msg + "\n\n"));
      }
    }
  } catch (err) {
    console.error("A stream failed", err);
    // Decide if one failure should abort the whole merged stream:
    // await writer.abort(err);
  } finally {
    activeStreams--;

    // Only close if you know you aren't adding any more streams dynamically
    // If you expect more later, you might want to remove this auto-close logic
    if (activeStreams === 0) {
      await writer.close();
    }
  }
}

// Example usage:
const res1 = await fetch("/sse/stream-1");
addDynamicStream(res1);

// 5 seconds later...
setTimeout(async () => {
  const res2 = await fetch("/sse/stream-2");
  addDynamicStream(res2);
}, 5000);

// You can immediately return this to the client
return new Response(aggr.readable, {
  headers: { "Content-Type": "text/event-stream" },
});
