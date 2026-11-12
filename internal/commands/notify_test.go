package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

func notifyPostData(t *testing.T, userId, message string) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(&model.Post{UserId: userId, Message: message})
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"post": string(encoded)}
}

func TestPostMatcherSubstringCaseInsensitiveByDefault(t *testing.T) {
	matcher, err := newPostMatcher("", "Hello.World", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !matcher.matchesEventData(notifyPostData(t, "u1", "say HELLO.world now")) {
		t.Error("expected case-insensitive substring match")
	}
	if matcher.matchesEventData(notifyPostData(t, "u1", "helloXworld")) {
		t.Error("substring must be literal, not a regex")
	}
}

func TestPostMatcherCaseSensitive(t *testing.T) {
	matcher, err := newPostMatcher("", "Hello", false, true)
	if err != nil {
		t.Fatal(err)
	}
	if !matcher.matchesEventData(notifyPostData(t, "u1", "Hello there")) {
		t.Error("expected exact-case match")
	}
	if matcher.matchesEventData(notifyPostData(t, "u1", "hello there")) {
		t.Error("expected case mismatch to be rejected")
	}
}

func TestPostMatcherRegex(t *testing.T) {
	matcher, err := newPostMatcher("", `@jcarter\b`, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if !matcher.matchesEventData(notifyPostData(t, "u1", "@jcarter hi")) {
		t.Error("expected @jcarter hi to match")
	}
	if matcher.matchesEventData(notifyPostData(t, "u1", "@jcarter2 hi")) {
		t.Error("expected @jcarter2 not to match")
	}
}

func TestPostMatcherInvalidRegex(t *testing.T) {
	if _, err := newPostMatcher("", "(", true, false); err == nil {
		t.Error("expected invalid regex error")
	}
}

func TestPostMatcherModifiersRequireMessage(t *testing.T) {
	for _, flags := range [][2]bool{{true, false}, {false, true}} {
		_, err := newPostMatcher("", "", flags[0], flags[1])
		if err == nil || !strings.Contains(err.Error(), "requires --message") && !strings.Contains(err.Error(), "require --message") {
			t.Errorf("flags %v: expected requires --message error, got %v", flags, err)
		}
	}
}

func TestPostMatcherUser(t *testing.T) {
	matcher, err := newPostMatcher("u1", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !matcher.matchesEventData(notifyPostData(t, "u1", "x")) {
		t.Error("expected author match")
	}
	if matcher.matchesEventData(notifyPostData(t, "u2", "x")) {
		t.Error("expected author mismatch")
	}
}

func TestPostMatcherUserAndMessageCombine(t *testing.T) {
	matcher, err := newPostMatcher("u1", "hi", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if matcher.matchesEventData(notifyPostData(t, "u2", "hi")) || matcher.matchesEventData(notifyPostData(t, "u1", "bye")) {
		t.Error("filters must combine with AND")
	}
	if !matcher.matchesEventData(notifyPostData(t, "u1", "hi")) {
		t.Error("expected both filters to match")
	}
}

func TestPostMatcherDropsEventsWithoutPost(t *testing.T) {
	matcher, err := newPostMatcher("u1", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !matcher.active() {
		t.Fatal("expected matcher to be active")
	}
	for _, data := range []map[string]any{nil, {}, {"post": "not json"}, {"post": 5}} {
		if matcher.matchesEventData(data) {
			t.Errorf("expected %v to be dropped", data)
		}
	}
	inactive, _ := newPostMatcher("", "", false, false)
	if inactive.active() {
		t.Error("expected empty matcher to be inactive")
	}
}

func TestEventJsonLineIsCompact(t *testing.T) {
	event := model.NewWebSocketEvent(model.WebsocketEventPosted, "", "channel1", "", nil, "")
	event.Add("post", `{"message":"a\nb"}`)
	line, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.ContainsAny(line, "\n") {
		t.Errorf("line contains newline: %q", line)
	}
	var decoded map[string]any
	if err := json.Unmarshal(line, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["event"] != "posted" {
		t.Errorf("event = %v", decoded["event"])
	}
	data, _ := decoded["data"].(map[string]any)
	if _, isString := data["post"].(string); !isString {
		t.Errorf("data.post should remain a string: %v", data["post"])
	}
}

func TestNextReconnectDelaySequence(t *testing.T) {
	expected := []time.Duration{1, 2, 4, 8, 16, 30, 30, 30}
	var delay time.Duration
	for index, seconds := range expected {
		delay = nextReconnectDelay(delay)
		if delay != seconds*time.Second {
			t.Fatalf("attempt %d: delay = %s, want %s", index, delay, seconds*time.Second)
		}
	}
}

func TestIsTokenRejected(t *testing.T) {
	unauthorized := model.NewAppError("test", "id", nil, "", http.StatusUnauthorized)
	serverError := model.NewAppError("test", "id", nil, "", http.StatusInternalServerError)
	cases := []struct {
		name     string
		response *model.Response
		err      error
		want     bool
	}{
		{"no error", &model.Response{StatusCode: http.StatusOK}, nil, false},
		{"401 response", &model.Response{StatusCode: http.StatusUnauthorized}, errors.New("x"), true},
		{"401 app error without response", nil, unauthorized, true},
		{"wrapped 401 app error", nil, fmt.Errorf("wrapped: %w", unauthorized), true},
		{"500 response", &model.Response{StatusCode: http.StatusInternalServerError}, serverError, false},
		{"403 response", &model.Response{StatusCode: http.StatusForbidden}, errors.New("x"), false},
		{"network error", nil, errors.New("dial tcp: connection refused"), false},
	}
	for _, testCase := range cases {
		if got := isTokenRejected(testCase.response, testCase.err); got != testCase.want {
			t.Errorf("%s: isTokenRejected = %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

func TestDescribeListenError(t *testing.T) {
	if got := describeListenError(nil); got != "connection closed by server" {
		t.Errorf("nil: %q", got)
	}
	listenError := model.NewAppError("NewWebSocketClient", "id", nil, "", http.StatusInternalServerError).
		Wrap(errors.New("websocket: close 1006 (abnormal closure): unexpected EOF"))
	if got := describeListenError(listenError); got != "websocket: close 1006 (abnormal closure): unexpected EOF" {
		t.Errorf("wrapped: %q", got)
	}
}

type reconnectorHarness struct {
	validations   []error
	connections   []error
	delays        []time.Duration
	reports       []string
	validateCalls int
	connectCalls  int
}

func (self *reconnectorHarness) build() *notifyReconnector {
	return &notifyReconnector{
		validateToken: func(ctx context.Context) (*model.Response, error) {
			index := self.validateCalls
			self.validateCalls++
			if index >= len(self.validations) || self.validations[index] == nil {
				return &model.Response{StatusCode: http.StatusOK}, nil
			}
			err := self.validations[index]
			var appError *model.AppError
			if errors.As(err, &appError) {
				return &model.Response{StatusCode: appError.StatusCode}, err
			}
			return nil, err
		},
		connect: func(ctx context.Context) (*model.WebSocketClient, error) {
			index := self.connectCalls
			self.connectCalls++
			if index < len(self.connections) && self.connections[index] != nil {
				return nil, self.connections[index]
			}
			return &model.WebSocketClient{}, nil
		},
		sleep: func(ctx context.Context, delay time.Duration) error {
			self.delays = append(self.delays, delay)
			return ctx.Err()
		},
		report: func(format string, arguments ...any) {
			self.reports = append(self.reports, fmt.Sprintf(format, arguments...))
		},
	}
}

func TestReconnectorBacksOffThenResetsAfterSuccess(t *testing.T) {
	down := errors.New("network down")
	harness := &reconnectorHarness{validations: []error{down, down, down}, connections: []error{nil}}
	reconnector := harness.build()

	if _, err := reconnector.run(context.Background(), "eof"); err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	if fmt.Sprint(harness.delays) != fmt.Sprint(want) {
		t.Fatalf("delays = %v, want %v", harness.delays, want)
	}
	wantReports := []string{
		"connection lost: eof; reconnecting in 1s",
		"reconnect failed: network down; reconnecting in 2s",
		"reconnect failed: network down; reconnecting in 4s",
		"reconnect failed: network down; reconnecting in 8s",
		"reconnected",
	}
	if fmt.Sprint(harness.reports) != fmt.Sprint(wantReports) {
		t.Fatalf("reports = %q, want %q", harness.reports, wantReports)
	}

	// After a successful connection the next loss starts from 1s again.
	harness.delays = nil
	if _, err := reconnector.run(context.Background(), "eof"); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(harness.delays) != fmt.Sprint([]time.Duration{time.Second}) {
		t.Fatalf("delays after reset = %v, want [1s]", harness.delays)
	}
}

func TestReconnectorRetriesConnectFailures(t *testing.T) {
	dialError := errors.New("dial refused")
	harness := &reconnectorHarness{connections: []error{dialError, dialError, nil}}
	if _, err := harness.build().run(context.Background(), "eof"); err != nil {
		t.Fatal(err)
	}
	if harness.connectCalls != 3 || len(harness.delays) != 3 {
		t.Fatalf("connectCalls = %d, delays = %v", harness.connectCalls, harness.delays)
	}
}

func TestReconnectorStopsOnRejectedToken(t *testing.T) {
	unauthorized := model.NewAppError("test", "id", nil, "token revoked", http.StatusUnauthorized)
	harness := &reconnectorHarness{validations: []error{errors.New("network down"), unauthorized}}
	_, err := harness.build().run(context.Background(), "eof")
	if err == nil || !strings.Contains(err.Error(), "rejected the token") {
		t.Fatalf("err = %v, want token rejected error", err)
	}
	if harness.validateCalls != 2 || harness.connectCalls != 0 {
		t.Fatalf("validateCalls = %d, connectCalls = %d", harness.validateCalls, harness.connectCalls)
	}
}

func TestReconnectorStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	harness := &reconnectorHarness{}
	_, err := harness.build().run(ctx, "eof")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if harness.validateCalls != 0 || harness.connectCalls != 0 {
		t.Fatalf("attempted work after cancellation: %d, %d", harness.validateCalls, harness.connectCalls)
	}
}

func TestSleepContextInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := sleepContext(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("sleepContext did not return promptly")
	}
}
