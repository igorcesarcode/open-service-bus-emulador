package server

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

const (
	// webSocketPath is where the SDKs open AMQP over WebSocket.
	webSocketPath = "/$servicebus/websocket"
	// webSocketProtocol is the subprotocol of the drafts of AMQP over
	// WebSocket, which the JavaScript and Python SDKs still send.
	webSocketProtocol = "AMQPWSB10"
	// webSocketProtocolFinal is the subprotocol of the final binding
	// (OASIS AMQP WebSocket Binding 1.0), which Azure.Amqp for .NET sends.
	webSocketProtocolFinal = "amqp"
)

// serveWebSocket serves an AMQP connection carried by WebSocket binary
// messages.
func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{webSocketProtocolFinal, webSocketProtocol},
		// Clients are SDKs, not browsers on other origins.
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.logger().Debug("WebSocket handshake failed", "remote", r.RemoteAddr, "err", err)
		return
	}
	if sp := ws.Subprotocol(); sp != webSocketProtocol && sp != webSocketProtocolFinal {
		ws.Close(websocket.StatusPolicyViolation, "the "+webSocketProtocolFinal+" or "+webSocketProtocol+" subprotocol is required")
		return
	}
	// AMQP limits frame sizes itself.
	ws.SetReadLimit(-1)
	// The connection outlives the request; closing the server closes it
	// through the AMQP server.
	s.serveAMQP(websocket.NetConn(context.Background(), ws, websocket.MessageBinary))
}
