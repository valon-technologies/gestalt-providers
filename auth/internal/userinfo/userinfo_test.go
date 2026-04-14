package userinfo

import "testing"

func TestEmailVerified(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value any
		want  bool
	}{
		{name: "bool true", value: true, want: true},
		{name: "bool false", value: false, want: false},
		{name: "string true", value: "true", want: true},
		{name: "string true mixed case", value: "TrUe", want: true},
		{name: "string false", value: "false", want: false},
		{name: "string garbage", value: "yes", want: false},
		{name: "number", value: 1, want: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := EmailVerified(tc.value); got != tc.want {
				t.Fatalf("EmailVerified(%v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestCheckAllowedDomains(t *testing.T) {
	t.Parallel()

	if err := CheckAllowedDomains("google", []string{"example.com"}, "user@example.com"); err != nil {
		t.Fatalf("expected allowed domain to pass, got %v", err)
	}
	if err := CheckAllowedDomains("google", []string{"example.com"}, "user@other.com"); err == nil {
		t.Fatal("expected disallowed domain to fail")
	}
	if err := CheckAllowedDomains("google", []string{"example.com"}, "not-an-email"); err == nil {
		t.Fatal("expected malformed email to fail")
	} else if err.Error() != "google auth: invalid email" {
		t.Fatalf("unexpected malformed email error: %v", err)
	}
}
