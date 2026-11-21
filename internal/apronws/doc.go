// Package apronws is a client for the apron WebSocket protocol.
//
// A Client dials the endpoint, records the server push, authenticates with a
// token before any other request is sent, keeps the connection alive with
// application-level pings on the server-suggested interval, and reconnects
// with exponential backoff (1s to 30s), re-authing on each attempt. Inbound
// pushes are decoded and delivered on Records. Pushes are not replayed across
// reconnects; set OnReconnect to re-run catch-up (e.g., History).
package apronws
