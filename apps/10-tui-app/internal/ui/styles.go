package ui

import (
	"charm.land/lipgloss/v2"

	"github.com/Maksim-Burtsev/go-reading/apps/10-tui-app/internal/task"
)

type styles struct {
	header     lipgloss.Style
	muted      lipgloss.Style
	pane       lipgloss.Style
	activePane lipgloss.Style
	row        lipgloss.Style
	cursorRow  lipgloss.Style
	doneRow    lipgloss.Style
	title      lipgloss.Style
	label      lipgloss.Style
	tag        lipgloss.Style
	overdue    lipgloss.Style
	priority   map[task.Priority]lipgloss.Style
}

func newStyles() styles {
	accent := lipgloss.Color("62")
	subtle := lipgloss.Color("241")
	pane := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(subtle).
		Padding(0, 1)

	return styles{
		header:     lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("230")).Background(accent).Padding(0, 1),
		muted:      lipgloss.NewStyle().Foreground(subtle),
		pane:       pane,
		activePane: pane.BorderForeground(accent),
		row:        lipgloss.NewStyle(),
		cursorRow:  lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("230")).Background(accent),
		doneRow:    lipgloss.NewStyle().Foreground(subtle).Strikethrough(true),
		title:      lipgloss.NewStyle().Bold(true).MarginBottom(1),
		label:      lipgloss.NewStyle().Foreground(subtle).Width(10),
		tag:        lipgloss.NewStyle().Foreground(lipgloss.Color("37")),
		overdue:    lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("203")),
		priority: map[task.Priority]lipgloss.Style{
			task.PriorityHigh:   lipgloss.NewStyle().Foreground(lipgloss.Color("203")),
			task.PriorityMedium: lipgloss.NewStyle().Foreground(lipgloss.Color("214")),
			task.PriorityLow:    lipgloss.NewStyle().Foreground(lipgloss.Color("109")),
		},
	}
}
