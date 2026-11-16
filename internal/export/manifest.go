package export

import (
	"strconv"
	"time"

	"github.com/TicketsBot-cloud/database"
)

const ManifestVersion = 2

// Manifest lists every source considered, including ones deliberately withheld, so a
// data-subject request can be answered with evidence.
type Manifest struct {
	Version     int                     `json:"version"`
	Kind        database.GdprExportType `json:"kind"`
	ExportId    int64                   `json:"export_id"`
	UserId      uint64                  `json:"user_id,string"`
	GuildIds    []string                `json:"guild_ids,omitempty"`
	Categories  []string                `json:"categories"`
	TicketIds   []int                   `json:"ticket_ids,omitempty"`
	GeneratedAt time.Time               `json:"generated_at"`
	Sources     []ManifestSource        `json:"sources"`
	Files       ManifestFiles           `json:"files"`
}

type ManifestFiles struct {
	Written int `json:"written"`
}

type ManifestSource struct {
	Category string `json:"category"`
	GuildId  uint64 `json:"guild_id,omitempty,string"`
	Name     string `json:"name"`
	Included bool   `json:"included"`
	Records  int    `json:"records"`
	Reason   string `json:"reason,omitempty"`
}

func NewManifest(kind database.GdprExportType, exportId int64, userId uint64, guildIds []uint64, categories []string, ticketIds []int) *Manifest {
	m := &Manifest{
		Version:     ManifestVersion,
		Kind:        kind,
		ExportId:    exportId,
		UserId:      userId,
		Categories:  categories,
		TicketIds:   ticketIds,
		GeneratedAt: time.Now().UTC(),
	}

	for _, guildId := range guildIds {
		m.GuildIds = append(m.GuildIds, strconv.FormatUint(guildId, 10))
	}

	return m
}

func (m *Manifest) Add(category string, guildId uint64, name string, records int) {
	m.Sources = append(m.Sources, ManifestSource{Category: category, GuildId: guildId, Name: name, Included: true, Records: records})
}

func (m *Manifest) Withhold(category string, guildId uint64, name, reason string) {
	m.Sources = append(m.Sources, ManifestSource{Category: category, GuildId: guildId, Name: name, Included: false, Reason: reason})
}
