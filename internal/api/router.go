package api

/*
Wires all routes (POST /notify, GET /notifications, GET /health, GET /metrics) and attaches middleware. Uses the chi router.
Why needed: separates routing from handler logic. handler.go stays focused on business logic, router.go stays focused on URL mapping.
*/

/*
Read it — it is short. Just wires routes to handlers with middleware applied.
*/
