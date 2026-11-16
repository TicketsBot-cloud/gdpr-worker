package export

import "testing"

func TestNormalise(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"2^53 stays a number", `{"a":9007199254740992}`, `{"a":9007199254740992}`},
		{"-2^53 stays a number", `{"a":-9007199254740992}`, `{"a":-9007199254740992}`},
		{"2^53+1 becomes a string", `{"a":9007199254740993}`, `{"a":"9007199254740993"}`},
		{"-(2^53+1) becomes a string", `{"a":-9007199254740993}`, `{"a":"-9007199254740993"}`},
		{"snowflake becomes a string", `{"a":123456789012345678}`, `{"a":"123456789012345678"}`},
		{"beyond int64 becomes a string", `{"a":18446744073709551615}`, `{"a":"18446744073709551615"}`},
		{"floats stay numbers", `{"a":1.5,"b":1e3}`, `{"a":1.5,"b":1e3}`},
		{"nested JSONB is walked", `{"a":{"b":[123456789012345678,{"c":123456789012345678}]}}`, `{"a":{"b":["123456789012345678",{"c":"123456789012345678"}]}}`},
		{"tokens are withheld", `{"token":"S","webhook_token":"S","name":"kept"}`, `{"name":"kept","token":"[withheld]","webhook_token":"[withheld]"}`},
		{"secret header value is withheld", `{"header_value":"S","is_secret":true}`, `{"header_value":"[withheld]","is_secret":true}`},
		{"plain header value is kept", `{"header_value":"v","is_secret":false}`, `{"header_value":"v","is_secret":false}`},
		{"audit header values are withheld", `{"new_data":{"headers":[{"name":"a","value":"S"}]}}`, `{"new_data":{"headers":[{"name":"a","value":"[withheld]"}]}}`},
		{"reviewer IDs are withheld", `{"old_data":{"reviewed_by":"1"},"new_data":{"reviewed_by":123456789012345678}}`, `{"new_data":{"reviewed_by":"[withheld]"},"old_data":{"reviewed_by":"[withheld]"}}`},
	} {
		got, err := Normalise(tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}

		if string(got) != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}
