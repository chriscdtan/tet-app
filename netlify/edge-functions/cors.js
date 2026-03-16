// File: netlify/edge-functions/cors.js

export default async (request, context) => {
  // 1. Intercept the invisible preflight OPTIONS request and auto-approve it
  if (request.method === "OPTIONS") {
    return new Response("ok", {
      status: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      },
    });
  }

  // 2. Let Netlify's normal proxy handle the actual GET/POST request (like your file upload)
  const response = await context.next();
  
  // 3. Attach CORS headers to Lark's response so your browser accepts the data
  response.headers.set("Access-Control-Allow-Origin", "*");
  return response;
};