package api

/*
Three middleware functions: RequestID (stamps every request with a unique ID), Logger (logs method/path/status/duration for every request), Recoverer (catches panics and returns 500 instead of crashing the server).
Why needed: without RequestID you cannot trace a single request through your logs. Without Recoverer one bad request can kill the entire server.
*/

/*
Read Logger and Recoverer. Understand the responseWriter wrapper — why we need it to capture the status code.
*/
