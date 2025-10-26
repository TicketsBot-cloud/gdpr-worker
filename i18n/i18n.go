package i18n

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Locale struct {
	IsoShortCode string
	IsoLongCode  string
	Messages     map[MessageId]string
}

var LocaleEnglish = &Locale{
	IsoShortCode: "en",
	IsoLongCode:  "en-GB",
}

var locales = make(map[string]*Locale)

func Init(localePath string) error {
	// Load English 
	if err := loadLocale(localePath, LocaleEnglish); err != nil {
		return fmt.Errorf("failed to load English locale: %w", err)
	}
	locales["en"] = LocaleEnglish
	locales["en-GB"] = LocaleEnglish

	// Load all other locale files
	files, err := os.ReadDir(localePath)
	if err != nil {
		return fmt.Errorf("failed to read locale directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}

		// Skip English as it's already loaded
		if file.Name() == "en-GB.json" {
			continue
		}

		// Extract ISO code from filename (e.g., "en-GB.json" -> "en-GB")
		isoLongCode := file.Name()[:len(file.Name())-5]
		isoShortCode := isoLongCode[:2]

		locale := &Locale{
			IsoShortCode: isoShortCode,
			IsoLongCode:  isoLongCode,
		}

		if err := loadLocale(localePath, locale); err != nil {
			fmt.Printf("Warning: Failed to load locale %s: %s\n", isoLongCode, err.Error())
			continue
		}

		locales[isoShortCode] = locale
		locales[isoLongCode] = locale
	}

	return nil
}

func loadLocale(basePath string, locale *Locale) error {
	path := fmt.Sprintf("%s/%s.json", basePath, locale.IsoLongCode)

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	messages, err := parseCrowdInFile(data)
	if err != nil {
		return err
	}

	locale.Messages = messages
	return nil
}

func parseCrowdInFile(data []byte) (map[MessageId]string, error) {
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}

	return parseCrowdInData("", parsed), nil
}

func parseCrowdInData(path string, data map[string]interface{}) map[MessageId]string {
	parsed := make(map[MessageId]string)

	for key, value := range data {
		var newPath string
		if key == "" {
			newPath = path
		} else if path == "" {
			newPath = key
		} else {
			newPath = fmt.Sprintf("%s.%s", path, key)
		}

		s, ok := value.(string)
		if ok {
			if s == "" {
				continue
			}

			parsed[MessageId(newPath)] = s
		} else if m, ok := value.(map[string]interface{}); ok {
			for k, v := range parseCrowdInData(newPath, m) {
				if v == "" {
					continue
				}

				parsed[k] = v
			}
		}
	}

	return parsed
}

func GetLocale(isoCode string) *Locale {
	if isoCode == "" {
		return LocaleEnglish
	}

	locale, ok := locales[isoCode]
	if !ok {
		return LocaleEnglish
	}

	return locale
}

func GetMessage(locale *Locale, id MessageId, format ...interface{}) string {
	if locale == nil {
		locale = LocaleEnglish
	}

	if locale.Messages == nil {
		if locale == LocaleEnglish {
			return fmt.Sprintf("Error: translations for language `%s` is missing", locale.IsoShortCode)
		}

		locale = LocaleEnglish
		return GetMessage(locale, id, format...)
	}

	value, ok := locale.Messages[id]
	if !ok || value == "" {
		if locale == LocaleEnglish {
			return fmt.Sprintf("error: translation for `%s` is missing", id)
		}

		return GetMessage(LocaleEnglish, id, format...)
	}

	return fmt.Sprintf(value, format...)
}
