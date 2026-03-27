package sender

/*
Contains APNSSender (iOS), FCMSender (Android), SMSSender (Twilio), EmailSender (SendGrid), and MockSender (for tests).
Each has a Send() method with a TODO comment showing where to drop the real SDK call.
Why needed: each channel has completely different third-party APIs.
Separating them means adding a new channel (WhatsApp) is just adding a new struct zero changes to worker or handler.
*/

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Durgendra-kumar/NotificationSystem/internal/domain"
)

// MockSender implements domain.Sender for testing and local development.
// It logs the notification instead of calling a real third-party API.
// Use this in unit tests and when you don't have API keys yet.
type MockSender struct {
	channel domain.Channel
	logger  *slog.Logger
}

// NewMockSender creates a mock sender for the given channel.
func NewMockSender(channel domain.Channel, logger *slog.Logger) *MockSender {
	return &MockSender{channel: channel, logger: logger}
}

func (s *MockSender) Send(ctx context.Context, n *domain.Notification) error {
	s.logger.Info("mock sender: would send notification",
		"channel", s.channel,
		"notification_id", n.ID,
		"user_id", n.UserID,
		"template_id", n.TemplateID,
	)
	return nil
}

func (s *MockSender) Channel() domain.Channel { return s.channel }

// APNSSender sends iOS push notifications via Apple Push Notification Service.
// Replace the body of Send() with the real APNs SDK call when you have credentials.
type APNSSender struct {
	// client *apns2.Client  ← uncomment when adding the real SDK
	logger *slog.Logger
}

func NewAPNSSender(logger *slog.Logger) *APNSSender {
	return &APNSSender{logger: logger}
}

func (s *APNSSender) Send(ctx context.Context, n *domain.Notification) error {
	if n.RecipientToken == "" {
		return fmt.Errorf("apns: notification %s has no device token", n.ID)
	}

	// TODO: replace with real APNs call
	// notification := &apns2.Notification{
	// 	DeviceToken: n.RecipientToken,
	// 	Topic:       "com.yourcompany.app",
	// 	Payload:     apns2.NewPayload().Alert(n.Payload["body"]),
	// }
	// _, err := s.client.PushWithContext(ctx, notification)

	s.logger.Info("apns: sent push notification",
		"notification_id", n.ID,
		"device_token", n.RecipientToken[:8]+"...", // never log full tokens
	)
	return nil
}

func (s *APNSSender) Channel() domain.Channel { return domain.ChannelIOS }

// FCMSender sends Android push notifications via Firebase Cloud Messaging.
type FCMSender struct {
	logger *slog.Logger
}

func NewFCMSender(logger *slog.Logger) *FCMSender {
	return &FCMSender{logger: logger}
}

func (s *FCMSender) Send(ctx context.Context, n *domain.Notification) error {
	if n.RecipientToken == "" {
		return fmt.Errorf("fcm: notification %s has no device token", n.ID)
	}

	// TODO: replace with real FCM call
	// message := &messaging.Message{
	// 	Token: n.RecipientToken,
	// 	Notification: &messaging.Notification{Body: n.Payload["body"]},
	// }
	// _, err := s.fcmClient.Send(ctx, message)

	s.logger.Info("fcm: sent push notification", "notification_id", n.ID)
	return nil
}

func (s *FCMSender) Channel() domain.Channel { return domain.ChannelAndroid }

// SMSSender sends SMS via Twilio.
type SMSSender struct {
	logger *slog.Logger
}

func NewSMSSender(logger *slog.Logger) *SMSSender {
	return &SMSSender{logger: logger}
}

func (s *SMSSender) Send(ctx context.Context, n *domain.Notification) error {
	if n.RecipientPhone == "" {
		return fmt.Errorf("sms: notification %s has no phone number", n.ID)
	}

	// TODO: replace with real Twilio call
	// client := twilio.NewRestClient()
	// params := &twilioApi.CreateMessageParams{}
	// params.SetTo(n.RecipientPhone)
	// params.SetBody(n.Payload["body"])

	s.logger.Info("sms: sent SMS", "notification_id", n.ID,
		"phone", mask(n.RecipientPhone))
	return nil
}

func (s *SMSSender) Channel() domain.Channel { return domain.ChannelSMS }

// EmailSender sends email via SendGrid.
type EmailSender struct {
	logger *slog.Logger
}

func NewEmailSender(logger *slog.Logger) *EmailSender {
	return &EmailSender{logger: logger}
}

func (s *EmailSender) Send(ctx context.Context, n *domain.Notification) error {
	if n.RecipientEmail == "" {
		return fmt.Errorf("email: notification %s has no email address", n.ID)
	}

	// TODO: replace with real SendGrid call
	// message := mail.NewSingleEmail(from, subject, to, plainTextContent, htmlContent)
	// client := sendgrid.NewSendClient(os.Getenv("SENDGRID_API_KEY"))
	// _, err := client.Send(message)

	s.logger.Info("email: sent email", "notification_id", n.ID,
		"to", mask(n.RecipientEmail))
	return nil
}

func (s *EmailSender) Channel() domain.Channel { return domain.ChannelEmail }

// mask hides the middle of a string — used to avoid logging PII in full.
func mask(s string) string {
	if len(s) <= 4 {
		return "****"
	}
	return s[:2] + "****" + s[len(s)-2:]
}
