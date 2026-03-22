package domain

/*
Defines the Sender interface: Send(ctx, notification) error and Channel() Channel. Just 10 lines.
Why needed: this is the SOLID Open/Closed principle in action. The worker never knows about APNs or SendGrid. It only knows about Sender. Add a WhatsApp channel — write one new struct, nothing else changes.
domain
*/

import "context"

// Sender is the interface every notification channel must implement.
// This is the core of SOLID's Open/Closed Principle:
//   - open for extension (add FCM, WebSocket, WhatsApp by adding a new struct)
//   - closed for modification (worker code never changes when you add a channel)
//
// To add a new channel:
//  1. Add the Channel constant in notification.go
//  2. Create a new file in sender/ that implements this interface
//  3. Register it in the worker — nothing else changes
type Sender interface {
	// Send delivers the notification to the third-party provider.
	// Implementations must be safe to call from multiple goroutines.
	Send(ctx context.Context, n *Notification) error

	// Channel returns which channel this sender handles.
	// Used by the dispatcher to route notifications to the right sender.
	Channel() Channel
}
