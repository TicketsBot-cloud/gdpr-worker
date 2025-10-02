package utils

import (
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
