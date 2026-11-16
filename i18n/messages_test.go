package i18n

import (
	"os"
	"regexp"
	"testing"
)

func TestGdprMessagesExistInEnglish(t *testing.T) {
	source, err := os.ReadFile("messages.go")
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile("../locale/en-GB.json")
	if err != nil {
		t.Fatal(err)
	}

	english, err := parseCrowdInFile(data)
	if err != nil {
		t.Fatal(err)
	}

	ids := regexp.MustCompile(`MessageId = "(gdpr\.[^"]*)"`).FindAllSubmatch(source, -1)
	if len(ids) == 0 {
		t.Fatal("no gdpr message IDs found in messages.go")
	}

	for _, match := range ids {
		if _, ok := english[MessageId(match[1])]; !ok {
			t.Errorf("%s is missing from en-GB.json", match[1])
		}
	}
}
