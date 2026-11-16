package callback

import (
	"errors"
	"slices"
	"testing"

	"github.com/TicketsBot-cloud/common/gdprrelay"
	"github.com/TicketsBot-cloud/gdl/objects/interaction/component"
	"github.com/TicketsBot-cloud/gdpr-worker/i18n"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
)

func TestExportResultComponents(t *testing.T) {
	if err := i18n.Init("../../locale"); err != nil {
		t.Fatal(err)
	}

	config.Conf.DashboardUrl = "https://dashboard.example"
	t.Cleanup(func() { config.Conf.DashboardUrl = "" })

	locale := i18n.LocaleEnglish

	for _, tc := range []struct {
		result ResultData
		text   string
		button bool
	}{
		{ResultData{RequestType: gdprrelay.RequestTypeExportGuild, GuildIds: []uint64{1}, ExportedFiles: 2, ExportId: 7}, i18n.GetMessage(locale, i18n.GdprCompletedExportGuild, "1") + i18n.GetMessage(locale, i18n.GdprCompletedExportReady), true},
		{ResultData{RequestType: gdprrelay.RequestTypeExportUser, ExportedFiles: 2, ExportId: 7}, i18n.GetMessage(locale, i18n.GdprCompletedExportUser) + i18n.GetMessage(locale, i18n.GdprCompletedExportReady), true},
		{ResultData{RequestType: gdprrelay.RequestTypeExportGuild, GuildIds: []uint64{1}}, i18n.GetMessage(locale, i18n.GdprCompletedExportGuild, "1") + i18n.GetMessage(locale, i18n.GdprCompletedExportNoData), false},
		{ResultData{RequestType: gdprrelay.RequestTypeExportGuild, GuildIds: []uint64{1}, ExportedFiles: 2, ExportId: 7, Error: errors.New("boom")}, i18n.GetMessage(locale, i18n.GdprCompletedExportGuild, "1") + i18n.GetMessage(locale, i18n.GdprCompletedError, "boom"), false},
		{ResultData{RequestType: gdprrelay.RequestTypeAllTranscripts, GuildIds: []uint64{1}, TranscriptsDeleted: 5, Error: errors.New("boom")}, i18n.GetMessage(locale, i18n.GdprCompletedAllTranscripts, "1", 5) + i18n.GetMessage(locale, i18n.GdprCompletedError, "boom"), false},
	} {
		container := (&Callback{}).buildResultComponents(locale, tc.result, nil)[0].ComponentData.(component.Container)

		var texts []string
		var buttons []component.Button
		for _, child := range container.Components {
			switch data := child.ComponentData.(type) {
			case component.TextDisplay:
				texts = append(texts, data.Content)
			case component.ActionRow:
				for _, button := range data.Components {
					buttons = append(buttons, button.ComponentData.(component.Button))
				}
			}
		}

		if !slices.Contains(texts, tc.text) {
			t.Errorf("texts %q, want %q", texts, tc.text)
		}

		if !tc.button {
			if len(buttons) > 0 {
				t.Errorf("%+v has buttons %+v", tc.result, buttons)
			}
			continue
		}

		if len(buttons) != 1 || buttons[0].Style != component.ButtonStyleLink || buttons[0].Url == nil ||
			*buttons[0].Url != "https://dashboard.example/gdpr?export_id=7" || buttons[0].Label != i18n.GetMessage(locale, i18n.GdprCompletedDownloadButton) {
			t.Errorf("buttons %+v, want one link to the export", buttons)
		}
	}
}

func TestNotificationStatus(t *testing.T) {
	for _, tc := range []struct {
		result ResultData
		want   string
	}{
		{ResultData{RequestType: gdprrelay.RequestTypeExportGuild, Error: errors.New("boom")}, "failed"},
		{ResultData{RequestType: gdprrelay.RequestTypeAllTranscripts, Error: errors.New("boom")}, "failed"},
		{ResultData{RequestType: gdprrelay.RequestTypeExportGuild}, "no_data"},
		{ResultData{RequestType: gdprrelay.RequestTypeExportUser}, "no_data"},
		{ResultData{RequestType: gdprrelay.RequestTypeExportUser, ExportedFiles: 3}, "completed"},
		{ResultData{RequestType: gdprrelay.RequestTypeAllTranscripts}, "completed"},
		{ResultData{RequestType: gdprrelay.RequestTypeSpecificMessages}, "completed"},
		{ResultData{RequestType: gdprrelay.RequestTypeAllMessages, MessagesDeleted: 4}, "completed"},
	} {
		if got := tc.result.NotificationStatus(); got != tc.want {
			t.Errorf("%+v: got %q, want %q", tc.result, got, tc.want)
		}
	}
}
