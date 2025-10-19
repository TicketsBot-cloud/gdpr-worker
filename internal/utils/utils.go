package utils

import (
	"crypto/sha256"
	"strconv"
	"fmt"

	"github.com/TicketsBot-cloud/gdl/objects/interaction/component"
)

type Colour int

const (
	Green Colour = iota
	Orange
	Red
)

func (c Colour) ToRGB() int {
	switch c {
	case Green:
		return 0x2ECC71
	case Orange:
		return 0xE67E22
	case Red:
		return 0xE74C3C
	default:
		return 0x2ECC71
	}
}

func BuildContainerWithComponents(colour Colour, title string, innerComponents []component.Component) component.Component {
	components := []component.Component{
		component.BuildTextDisplay(component.TextDisplay{
			Content: fmt.Sprintf("### %s", title),
		}),
		component.BuildSeparator(component.Separator{}),
	}

	components = append(components, innerComponents...)

	accentColor := colour.ToRGB()
	return component.BuildContainer(component.Container{
		AccentColor: &accentColor,
		Components:  components,
	})
}

func FormatGuildDisplay(guildId uint64, guildNames map[uint64]string) string {
	if name, ok := guildNames[guildId]; ok && name != "" {
		return name + " (" + strconv.FormatUint(guildId, 10) + ")"
	}
	return strconv.FormatUint(guildId, 10)
}

func ScrambleUserId(userId uint64) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d", userId)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func GetRequestTypeName(requestType int) string {
	switch requestType {
	case 0:
		return "AllTranscripts"
	case 1:
		return "SpecificTranscripts"
	case 2:
		return "AllMessages"
	case 3:
		return "SpecificMessages"
	default:
		return fmt.Sprintf("Unknown(%d)", requestType)
	}
}
