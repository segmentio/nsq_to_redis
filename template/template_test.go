package template_test

import (
	"encoding/json"
	"fmt"
	"testing"

	interpolate "github.com/segmentio/go-interpolate"
	"github.com/segmentio/nsq_to_redis/template"
)

func TestNewInvalid(t *testing.T) {
	testCases := []struct {
		format string
	}{
		{"{"},
		{"{foo"},
		{"da:sad{foo"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%q", tc.format), func(t *testing.T) {
			_, err := template.New(tc.format)
			if err != template.ErrMissingClosingBrace {
				t.Error("expected New to fail because closing brace was missing but didn't")
			}
		})
	}
}

func TestValidTemplates(t *testing.T) {
	testCases := []struct {
		format   string
		data     string
		expected string
	}{
		{"}", `{}`, "}"},
		{"", `{}`, ""},
		{"foo:bar:baz", `{}`, "foo:bar:baz"},
		{"foo:{bar}", `{"bar":"baz"}`, "foo:baz"},
		{"{foo}:{bar}:baz", `{"foo":"f","bar":"b"}`, "f:b:baz"},
		{"foo:{bar.baz}", `{"bar":{"baz":"b"}}`, "foo:b"},
		{"stream:{foo}:{bar}", `{"foo":"f","bar":"b"}`, "stream:f:b"},
		{"stream:project:{projectId}:ingress", `{}`, "stream:project:null:ingress"},
		{"integration-errors:project:{projectId}:ingress", `{"projectId":"p"}`, "integration-errors:project:p:ingress"},
		{"stream:project:{projectId}:ingress", `{"projectId":"foo"}`, "stream:project:foo:ingress"},
		{"stream:persist:{projectId}:ingress", `{"projectId":"foo"}`, "stream:persist:foo:ingress"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%q.Eval(`%s`)", tc.format, tc.data), func(t *testing.T) {
			tmpl, err := template.New(tc.format)
			if err != nil {
				t.Error("expected New to not error, but did: %v", err)
			}

			got, err := tmpl.Eval(tc.data)
			if err != nil {
				t.Errorf("expected Eval to not error, but did: %v", err)
			}

			if got != tc.expected {
				t.Errorf("expected eval to return %q, but got: %q", tc.expected, got)
			}
		})
	}
}

var benchmarkData = []byte(`{
  "anonymousId": "075100b8-0011-4c87-8731-450e5e41858a",
  "context": {
    "app": {
      "build": 175,
      "name": "Car Loan Calculator",
      "namespace": "com.boondoggle.autocalc",
      "version": "1.7.5"
    },
    "device": {
      "adTrackingEnabled": true,
      "advertisingId": "khisaldas>",
      "id": "dsadas",
      "manufacturer": "samsung",
      "model": "SAMSUNG-SM-G930A",
      "name": "heroqlteatt",
      "type": "android"
    },
    "library": {
      "name": "analytics-android",
      "version": "4.2.4"
    },
    "locale": "en-US",
    "network": {
      "bluetooth": false,
      "carrier": "AT&T",
      "cellular": false,
      "wifi": true
    },
    "os": {
      "name": "Android",
      "version": "7.0"
    },
    "screen": {
      "density": 3,
      "height": 1920,
      "width": 1080
    },
    "timezone": "America/Chicago",
    "traits": {
      "anonymousId": "89b2-d-sads-da-sda-d12bdsad"
    },
    "userAgent": "Dalvik/2.1.0 (Linux; U; Android 7.0; SAMSUNG-SM-G930A Build/NRD90M)",
    "ip": "99.185.26.86"
  },
  "event": "Application Launched",
  "integrations": {
    "Mixpanel": false,
    "MoEngage": false
  },
  "messageId": "ef5b39cc-ad0a-4a80-8b43-18573a2b6b15",
  "properties": {
    "event_id": "33e95185-596c-4097-a597-e3cb3282d686",
    "event_number": 34,
    "via_analytics_android": "true"
  },
  "timestamp": "2017-04-14T21:49:19.549Z",
  "type": "track",
  "writeKey": "xNb9e1cMp52JPelx7gvUo5kwR8vPxjcF",
  "sentAt": "2017-04-14T21:49:18.000Z",
  "receivedAt": "2017-04-14T21:49:19.549Z",
  "originalTimestamp": "2017-04-14T16:49:18-0500"
}`)

func BenchmarkTemplate(b *testing.B) {
	tmpl, err := template.New("stream:project:{projectId}:ingress")
	if err != nil {
		b.Error(err)
	}

	for i := 0; i < b.N; i++ {
		tmpl.Eval(string(benchmarkData))
	}
}

func BenchmarkInterpolate(b *testing.B) {
	tmpl, err := interpolate.New("stream:project:{projectId}:ingress")
	if err != nil {
		b.Error(err)
	}

	for i := 0; i < b.N; i++ {
		var m map[string]interface{}
		json.Unmarshal(benchmarkData, &m)
		tmpl.Eval(m)
	}
}
