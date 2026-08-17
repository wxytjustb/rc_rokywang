package adapters

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"notification-delivery/internal/httpclient"
	"notification-delivery/internal/provider"
)

func TestLarkBotValidateAcceptsDocumentedMessageTypes(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "text", payload: `{"msg_type":"text","content":{"text":"request example"}}`},
		{name: "post", payload: `{"msg_type":"post","content":{"post":{"zh_cn":{"title":"更新","content":[[{"tag":"text","text":"内容"}]]}}}}`},
		{name: "image", payload: `{"msg_type":"image","content":{"image_key":"img_key"}}`},
		{name: "share chat", payload: `{"msg_type":"share_chat","content":{"share_chat_id":"oc_chat"}}`},
		{name: "interactive", payload: `{"msg_type":"interactive","card":{"elements":[{"tag":"div"}]}}`},
		{name: "legacy text", payload: `{"text":"request example"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := (LarkBot{}).Validate(actionSend, json.RawMessage(tt.payload)); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestLarkBotValidateRejectsInvalidMessages(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "empty object", payload: `{}`},
		{name: "empty text", payload: `{"msg_type":"text","content":{"text":" "}}`},
		{name: "unsupported type", payload: `{"msg_type":"audio","content":{}}`},
		{name: "post without locale", payload: `{"msg_type":"post","content":{"post":{}}}`},
		{name: "interactive without card", payload: `{"msg_type":"interactive"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := (LarkBot{}).Validate(actionSend, json.RawMessage(tt.payload)); err == nil {
				t.Fatal("Validate() error = nil, want validation failure")
			}
		})
	}
}

func TestBuildLarkRequestBodyUsesOfficialSchemaAndSignature(t *testing.T) {
	body, err := buildLarkRequestBody(
		json.RawMessage(`{"text":"request example"}`),
		"demo",
		time.Unix(100, 0),
	)
	if err != nil {
		t.Fatalf("buildLarkRequestBody() error = %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got["timestamp"] != "100" {
		t.Errorf("timestamp = %v, want 100", got["timestamp"])
	}
	if got["sign"] != "jquNHnVOwmDRfw+vqTIrY5dooJAgi5EcRtLsQE4wfXg=" {
		t.Errorf("sign = %v", got["sign"])
	}
	if got["msg_type"] != "text" {
		t.Errorf("msg_type = %v, want text", got["msg_type"])
	}
	content, ok := got["content"].(map[string]any)
	if !ok || content["text"] != "request example" {
		t.Errorf("content = %#v", got["content"])
	}
}

func TestLarkBotRejectsOversizedRequest(t *testing.T) {
	payload := json.RawMessage(`{"msg_type":"text","content":{"text":"` + strings.Repeat("x", larkMaxRequestBytes) + `"}}`)
	if err := (LarkBot{}).Validate(actionSend, payload); err == nil || !strings.Contains(err.Error(), "maximum request body") {
		t.Fatalf("Validate() error = %v, want size-limit error", err)
	}
}

func TestInterpretLarkResponse(t *testing.T) {
	tests := []struct {
		name                string
		statusCode          int
		body                string
		success             bool
		availabilityFailure bool
		errorClass          string
	}{
		{name: "success", statusCode: 200, body: `{"code":0,"msg":"success"}`, success: true},
		{name: "rate limited", statusCode: 200, body: `{"code":11232,"msg":"rate limited"}`, availabilityFailure: true, errorClass: "LARK_CODE_11232"},
		{name: "bad request", statusCode: 200, body: `{"code":9499,"msg":"Bad Request"}`, errorClass: "LARK_CODE_9499"},
		{name: "ip not allowed", statusCode: 200, body: `{"code":19022,"msg":"Ip Not Allowed"}`, errorClass: "LARK_CODE_19022"},
		{name: "missing code", statusCode: 200, body: `{}`, availabilityFailure: true, errorClass: "UNEXPECTED_VENDOR_BODY"},
		{name: "http rejected", statusCode: 400, body: ``, errorClass: "LARK_HTTP_ERROR"},
		{name: "http timeout", statusCode: 408, body: ``, availabilityFailure: true, errorClass: "LARK_HTTP_ERROR"},
		{name: "http rate limited", statusCode: 429, body: ``, availabilityFailure: true, errorClass: "LARK_HTTP_ERROR"},
		{name: "http unavailable", statusCode: 503, body: ``, availabilityFailure: true, errorClass: "LARK_HTTP_ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := interpretLarkResponse(&httpclient.Response{StatusCode: tt.statusCode, Body: []byte(tt.body)})
			if got.Success != tt.success || got.AvailabilityFailure != tt.availabilityFailure || got.ErrorClass != tt.errorClass {
				t.Fatalf("result = %+v, want success=%t availability_failure=%t error_class=%q", got, tt.success, tt.availabilityFailure, tt.errorClass)
			}
		})
	}
}

func TestLarkBotTransportFailureAffectsAvailability(t *testing.T) {
	result, err := (LarkBot{}).sendMessage(context.Background(), provider.ActionContext{
		// TCP port 1 is reserved and has no service in the test environment.
		Path:       "http://127.0.0.1:1/open-apis/bot/v2/hook/test",
		Method:     "POST",
		TimeoutMs:  500,
		HTTPClient: httpclient.New(1024),
	}, json.RawMessage(`{"text":"test"}`))
	if err != nil {
		t.Fatalf("sendMessage() error = %v", err)
	}
	if result == nil || !result.AvailabilityFailure || result.Success {
		t.Fatalf("transport result = %+v", result)
	}
}

func TestLarkBotMissingHTTPClientIsAdapterError(t *testing.T) {
	result, err := (LarkBot{}).sendMessage(context.Background(), provider.ActionContext{
		Path:   "http://127.0.0.1/open-apis/bot/v2/hook/test",
		Method: "POST",
	}, json.RawMessage(`{"text":"test"}`))
	if err == nil || result != nil {
		t.Fatalf("sendMessage() = (%+v, %v), want adapter error", result, err)
	}
}

func TestValidateLarkWebhookURL(t *testing.T) {
	for _, valid := range []string{
		"https://open.larksuite.com/open-apis/bot/v2/hook/test",
		"https://open.feishu.cn/open-apis/bot/v2/hook/test",
		"http://127.0.0.1:18080/open-apis/bot/v2/hook/test",
	} {
		if err := validateLarkWebhookURL(valid); err != nil {
			t.Errorf("validateLarkWebhookURL(%q) error = %v", valid, err)
		}
	}
	for _, invalid := range []string{
		"http://open.larksuite.com/open-apis/bot/v2/hook/test",
		"https://open.larksuite.com/not-a-webhook",
		"https://open.larksuite.com/open-apis/bot/v2/hook/test?secret=bad",
	} {
		if err := validateLarkWebhookURL(invalid); err == nil {
			t.Errorf("validateLarkWebhookURL(%q) error = nil", invalid)
		}
	}
}
