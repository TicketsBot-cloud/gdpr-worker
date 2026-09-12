package export

import "time"

const ManifestVersion = 1

// Manifest lists every source considered, including ones deliberately withheld, so a
// data-subject request can be answered with evidence.
type Manifest struct {
	Version     int           `json:"version"`
	Kind        string        `json:"kind"`
	GeneratedAt time.Time     `json:"generated_at"`
	Sources     []Source      `json:"sources"`
	Files       ManifestFiles `json:"files"`
}

type ManifestFiles struct {
	Written  int      `json:"written"`
	Withheld []string `json:"withheld_too_large,omitempty"`
}

type Source struct {
	Name     string `json:"name"`
	Included bool   `json:"included"`
	Records  int    `json:"records"`
	Reason   string `json:"reason,omitempty"`
}

func (m *Manifest) Add(name string, records int) {
	m.Sources = append(m.Sources, Source{Name: name, Included: true, Records: records})
}

func (m *Manifest) Withhold(name, reason string) {
	m.Sources = append(m.Sources, Source{Name: name, Included: false, Reason: reason})
}
