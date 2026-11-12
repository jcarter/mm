package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/mattermost/mattermost/server/public/model"
	"github.com/spf13/cobra"
	"github.com/ziyan/mm/internal/client"
	"github.com/ziyan/mm/internal/printer"
)

// postMatcher filters events by the post they carry: author and message.
type postMatcher struct {
	userId  string
	pattern *regexp.Regexp
}

// newPostMatcher builds a matcher. An empty userId and message match everything.
func newPostMatcher(userId, message string, isRegex, isCaseSensitive bool) (*postMatcher, error) {
	if message == "" && (isRegex || isCaseSensitive) {
		return nil, fmt.Errorf("commands: --regex and --case-sensitive require --message")
	}
	matcher := &postMatcher{userId: userId}
	if message == "" {
		return matcher, nil
	}
	expression := message
	if !isRegex {
		expression = regexp.QuoteMeta(message)
	}
	if !isCaseSensitive {
		expression = "(?i)" + expression
	}
	pattern, err := regexp.Compile(expression)
	if err != nil {
		return nil, fmt.Errorf("commands: compiling --message: %w", err)
	}
	matcher.pattern = pattern
	return matcher, nil
}

// active reports whether any post filter is configured.
func (self *postMatcher) active() bool {
	return self.userId != "" || self.pattern != nil
}

// matchesPost reports whether the post satisfies every configured filter.
func (self *postMatcher) matchesPost(post *model.Post) bool {
	if self.userId != "" && post.UserId != self.userId {
		return false
	}
	if self.pattern != nil && !self.pattern.MatchString(post.Message) {
		return false
	}
	return true
}

// matchesEventData extracts the post from event data and matches it. Events
// without a parseable post never match.
func (self *postMatcher) matchesEventData(data map[string]any) bool {
	postJson, _ := data["post"].(string)
	if postJson == "" {
		return false
	}
	var post model.Post
	if err := json.Unmarshal([]byte(postJson), &post); err != nil {
		return false
	}
	return self.matchesPost(&post)
}

func init() {
	notifyCommand := &cobra.Command{
		Use:   "notify",
		Short: "Stream real-time notifications via WebSocket",
		RunE:  notifyRun,
	}
	notifyCommand.Flags().StringArray("event", nil, "Filter to specific event types (e.g., posted, typing)")
	notifyCommand.Flags().String("channel", "", "Filter to a specific channel name")
	notifyCommand.Flags().String("user", "", "Only events whose post was written by this username")
	notifyCommand.Flags().String("message", "", "Only events whose post message matches this text (case-insensitive substring)")
	notifyCommand.Flags().Bool("regex", false, "Treat --message as a regular expression")
	notifyCommand.Flags().Bool("case-sensitive", false, "Match case exactly")

	rootCommand.AddCommand(notifyCommand)
}

// Reconnect backoff bounds: the first retry waits the initial delay and each
// further failure doubles it up to the maximum.
const (
	notifyReconnectInitialDelay = time.Second
	notifyReconnectMaximumDelay = 30 * time.Second
)

// nextReconnectDelay returns the delay to wait before the next reconnect
// attempt given the delay used for the previous attempt. A zero (or smaller
// than initial) previous delay means no attempt has failed yet and yields the
// initial delay; the result never exceeds the maximum delay.
func nextReconnectDelay(previous time.Duration) time.Duration {
	if previous < notifyReconnectInitialDelay {
		return notifyReconnectInitialDelay
	}
	next := previous * 2
	if next > notifyReconnectMaximumDelay {
		return notifyReconnectMaximumDelay
	}
	return next
}

// isTokenRejected reports whether a reconnect precheck failure means the
// server rejected the token (HTTP 401). Such failures are fatal; every other
// failure (network down, 5xx, timeouts) is retryable.
func isTokenRejected(response *model.Response, err error) bool {
	if err == nil {
		return false
	}
	if response != nil && response.StatusCode == http.StatusUnauthorized {
		return true
	}
	var appError *model.AppError
	return errors.As(err, &appError) && appError.StatusCode == http.StatusUnauthorized
}

// describeListenError turns the error the websocket client recorded when its
// read loop ended into a human-readable reason.
func describeListenError(listenError *model.AppError) string {
	if listenError == nil {
		return "connection closed by server"
	}
	if cause := listenError.Unwrap(); cause != nil {
		return cause.Error()
	}
	return listenError.Error()
}

// notifyPingTimeoutReason is reported when the server stopped sending pings.
const notifyPingTimeoutReason = "no ping from server (connection timed out)"

// sleepContext waits for the delay or the context, whichever ends first.
func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// connectWebSocket dials the websocket and starts its read loop. The dial
// itself is not cancellable, so it runs in the background and is abandoned
// (and closed once it finishes) when the context ends first.
func connectWebSocket(ctx context.Context, websocketUrl, token string) (*model.WebSocketClient, error) {
	type result struct {
		client *model.WebSocketClient
		err    error
	}
	results := make(chan result, 1)
	go func() {
		websocketClient, err := model.NewWebSocketClient4(websocketUrl, token)
		results <- result{websocketClient, err}
	}()
	select {
	case <-ctx.Done():
		go func() {
			if outcome := <-results; outcome.client != nil {
				outcome.client.Close()
			}
		}()
		return nil, ctx.Err()
	case outcome := <-results:
		if outcome.err != nil {
			return nil, outcome.err
		}
		outcome.client.Listen()
		return outcome.client, nil
	}
}

// notifyReconnector re-establishes the websocket after it was lost. Its
// dependencies are injected so the retry policy can be tested.
type notifyReconnector struct {
	// validateToken makes a cheap authenticated request.
	validateToken func(ctx context.Context) (*model.Response, error)
	connect       func(ctx context.Context) (*model.WebSocketClient, error)
	sleep         func(ctx context.Context, delay time.Duration) error
	report        func(format string, arguments ...any)
}

// run retries until a connection is established, the token is rejected, or
// the context ends. The backoff restarts at the initial delay on every call,
// i.e. after each successful connection.
func (self *notifyReconnector) run(ctx context.Context, reason string) (*model.WebSocketClient, error) {
	var delay time.Duration
	label := "connection lost"
	for {
		delay = nextReconnectDelay(delay)
		self.report("%s: %s; reconnecting in %s", label, reason, delay)
		if err := self.sleep(ctx, delay); err != nil {
			return nil, err
		}
		label = "reconnect failed"

		response, err := self.validateToken(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if isTokenRejected(response, err) {
				return nil, fmt.Errorf("commands: reconnecting: server rejected the token (HTTP 401), it was revoked or has expired: %w", err)
			}
			reason = err.Error()
			continue
		}

		websocketClient, err := self.connect(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			reason = describeConnectError(err)
			continue
		}
		self.report("reconnected")
		return websocketClient, nil
	}
}

// describeConnectError unwraps the websocket client's generic AppError so the
// underlying dial failure is shown.
func describeConnectError(err error) string {
	var appError *model.AppError
	if errors.As(err, &appError) {
		return describeListenError(appError)
	}
	return err.Error()
}

func notifyRun(command *cobra.Command, arguments []string) error {
	apiClient, server, err := client.New()
	if err != nil {
		return err
	}
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	username, _ := command.Flags().GetString("user")
	message, _ := command.Flags().GetString("message")
	isRegex, _ := command.Flags().GetBool("regex")
	isCaseSensitive, _ := command.Flags().GetBool("case-sensitive")

	var userId string
	if username = strings.TrimPrefix(username, "@"); username != "" {
		user, _, err := apiClient.GetUserByUsername(ctx, username, "")
		if err != nil {
			return fmt.Errorf("commands: resolving --user %q: %w", username, err)
		}
		userId = user.Id
	}
	matcher, err := newPostMatcher(userId, message, isRegex, isCaseSensitive)
	if err != nil {
		return err
	}

	websocketUrl := client.WebSocketURL(server.URL)

	// Status lines go to stderr in JSON mode so stdout carries only events.
	statusWriter := printer.Stdout
	if printer.JSONOutput {
		statusWriter = printer.Stderr
	}

	websocketClient, err := connectWebSocket(ctx, websocketUrl, server.Token)
	if err != nil {
		if ctx.Err() != nil {
			_, _ = fmt.Fprintln(statusWriter, "\nDisconnected.")
			return nil
		}
		return fmt.Errorf("commands: connecting to websocket: %w", err)
	}
	defer func() { websocketClient.Close() }()

	reconnector := &notifyReconnector{
		validateToken: func(ctx context.Context) (*model.Response, error) {
			_, response, err := apiClient.GetMe(ctx, "")
			return response, err
		},
		connect: func(ctx context.Context) (*model.WebSocketClient, error) {
			return connectWebSocket(ctx, websocketUrl, server.Token)
		},
		sleep: sleepContext,
		report: func(format string, arguments ...any) {
			_, _ = fmt.Fprintf(statusWriter, format+"\n", arguments...)
		},
	}
	// reconnect replaces the lost websocket client. It returns a nil error and
	// a true interrupted flag when the user asked to stop meanwhile.
	reconnect := func(reason string) (interrupted bool, err error) {
		lostClient := websocketClient
		// Closing can block on a dead connection; do not hold up reconnecting.
		go lostClient.Close()
		newClient, err := reconnector.run(ctx, reason)
		if err != nil {
			if ctx.Err() != nil {
				return true, nil
			}
			return false, err
		}
		websocketClient = newClient
		return false, nil
	}

	eventFilter, _ := command.Flags().GetStringArray("event")
	channelFilter, _ := command.Flags().GetString("channel")

	var channelFilterId string
	if channelFilter != "" {
		channelId, err := resolveChannelId(ctx, apiClient, server.TeamID, channelFilter)
		if err != nil {
			return fmt.Errorf("commands: resolving --channel filter: %w", err)
		}
		channelFilterId = channelId
	}

	eventFilterSet := make(map[string]bool)
	for _, eventType := range eventFilter {
		eventFilterSet[eventType] = true
	}

	userCache := make(map[string]string)
	resolveUsername := func(userId string) string {
		if username, ok := userCache[userId]; ok {
			return username
		}
		user, _, err := apiClient.GetUser(ctx, userId, "")
		if err == nil {
			userCache[userId] = user.Username
			return user.Username
		}
		return userId[:8]
	}

	channelCache := make(map[string]string)
	resolveChannelName := func(channelId string) string {
		if channelName, ok := channelCache[channelId]; ok {
			return channelName
		}
		channel, _, err := apiClient.GetChannel(ctx, channelId)
		if err == nil {
			channelCache[channelId] = channel.DisplayName
			return channel.DisplayName
		}
		return channelId[:8]
	}

	bold := color.New(color.Bold)
	dim := color.New(color.Faint)

	_, _ = fmt.Fprintln(statusWriter, "Listening for events... (Ctrl+C to stop)")

	for {
		select {
		case <-ctx.Done():
			_, _ = fmt.Fprintln(statusWriter, "\nDisconnected.")
			return nil

		case <-websocketClient.PingTimeoutChannel:
			interrupted, err := reconnect(notifyPingTimeoutReason)
			if err != nil {
				return err
			}
			if interrupted {
				_, _ = fmt.Fprintln(statusWriter, "\nDisconnected.")
				return nil
			}
			continue

		case event, ok := <-websocketClient.EventChannel:
			if !ok {
				interrupted, err := reconnect(describeListenError(websocketClient.ListenError))
				if err != nil {
					return err
				}
				if interrupted {
					_, _ = fmt.Fprintln(statusWriter, "\nDisconnected.")
					return nil
				}
				continue
			}

			eventType := string(event.EventType())

			if len(eventFilterSet) > 0 && !eventFilterSet[eventType] {
				continue
			}
			if channelFilterId != "" {
				broadcast := event.GetBroadcast()
				if broadcast != nil && broadcast.ChannelId != channelFilterId {
					continue
				}
			}

			if matcher.active() && !matcher.matchesEventData(event.GetData()) {
				continue
			}

			if printer.JSONOutput {
				line, err := event.ToJSON()
				if err != nil {
					return fmt.Errorf("commands: encoding event: %w", err)
				}
				_, _ = fmt.Fprintf(printer.Stdout, "%s\n", line)
				continue
			}

			switch eventType {
			case "posted":
				data := event.GetData()
				postJson, _ := data["post"].(string)
				var post model.Post
				if err := json.Unmarshal([]byte(postJson), &post); err != nil {
					continue
				}
				channelName, _ := data["channel_display_name"].(string)
				if channelName == "" {
					channelName = resolveChannelName(post.ChannelId)
				}
				senderName, _ := data["sender_name"].(string)
				if senderName == "" {
					senderName = resolveUsername(post.UserId)
				}
				senderName = strings.TrimPrefix(senderName, "@")

				prefix := ""
				if post.RootId != "" {
					prefix = dim.Sprint("[reply] ")
				}

				_, _ = fmt.Fprintf(printer.Stdout, "%s %s%s %s: %s\n",
					dim.Sprint(printer.FormatTime(post.CreateAt)),
					prefix,
					bold.Sprintf("#%s", channelName),
					bold.Sprint(senderName),
					post.Message,
				)

			case "typing":
				data := event.GetData()
				userId, _ := data["user_id"].(string)
				broadcast := event.GetBroadcast()
				channelId := ""
				if broadcast != nil {
					channelId = broadcast.ChannelId
				}
				_, _ = fmt.Fprintf(printer.Stdout, "%s #%s %s is typing...\n",
					dim.Sprint(printer.FormatTime(0)),
					resolveChannelName(channelId),
					resolveUsername(userId),
				)

			case "status_change":
				data := event.GetData()
				userId, _ := data["user_id"].(string)
				status, _ := data["status"].(string)
				_, _ = fmt.Fprintf(printer.Stdout, "%s %s is now %s\n",
					dim.Sprint("status"),
					resolveUsername(userId),
					status,
				)

			case "reaction_added", "reaction_removed":
				data := event.GetData()
				reactionJson, _ := data["reaction"].(string)
				var reaction model.Reaction
				if err := json.Unmarshal([]byte(reactionJson), &reaction); err != nil {
					continue
				}
				action := "reacted"
				if eventType == "reaction_removed" {
					action = "unreacted"
				}
				_, _ = fmt.Fprintf(printer.Stdout, "%s %s %s :%s: on %s\n",
					dim.Sprint("reaction"),
					resolveUsername(reaction.UserId),
					action,
					reaction.EmojiName,
					reaction.PostId[:8],
				)

			case "channel_created", "channel_deleted", "channel_updated":
				broadcast := event.GetBroadcast()
				channelId := ""
				if broadcast != nil {
					channelId = broadcast.ChannelId
				}
				_, _ = fmt.Fprintf(printer.Stdout, "%s channel %s: %s\n",
					dim.Sprint("channel"),
					eventType,
					resolveChannelName(channelId),
				)

			case "user_added", "user_removed":
				data := event.GetData()
				userId, _ := data["user_id"].(string)
				broadcast := event.GetBroadcast()
				channelId := ""
				if broadcast != nil {
					channelId = broadcast.ChannelId
				}
				_, _ = fmt.Fprintf(printer.Stdout, "%s %s %s in %s\n",
					dim.Sprint("member"),
					resolveUsername(userId),
					strings.TrimPrefix(eventType, "user_"),
					resolveChannelName(channelId),
				)

			default:
				_, _ = fmt.Fprintf(printer.Stdout, "%s %s\n",
					dim.Sprint("event"),
					eventType,
				)
			}
		}
	}
}
