package ui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/Maksim-Burtsev/go-reading/apps/10-tui-app/internal/task"
)

// View implements tea.Model.
func (m Model) View() tea.View {
	v := tea.NewView("")
	v.AltScreen = true
	v.WindowTitle = "tasks"
	if m.width == 0 || m.height == 0 {
		return v
	}

	listWidth, detailsWidth := m.paneWidths()
	listPane, detailsPane := m.styles.pane, m.styles.pane
	if m.focus == focusDetails {
		detailsPane = m.styles.activePane
	} else {
		listPane = m.styles.activePane
	}
	frameX, frameY := listPane.GetFrameSize()
	body := lipgloss.JoinHorizontal(lipgloss.Top,
		listPane.Width(listWidth).Height(m.bodyHeight()).
			Render(m.listView(listWidth-frameX, m.bodyHeight()-frameY)),
		detailsPane.Width(detailsWidth).Height(m.bodyHeight()).
			Render(m.details.View()),
	)

	v.SetContent(lipgloss.JoinVertical(lipgloss.Left, m.headerView(), body, m.footerView()))
	return v
}

func (m Model) headerView() string {
	done := 0
	for _, t := range m.tasks {
		if t.Status == task.StatusDone {
			done++
		}
	}
	summary := fmt.Sprintf("%d of %d shown · %d done", len(m.visible), len(m.tasks), done)
	if q := strings.TrimSpace(m.filter.Value()); q != "" && m.focus != focusFilter {
		summary += fmt.Sprintf(" · filter %q", q)
	}
	if m.modified {
		summary += " · unsaved"
	}
	line := m.styles.header.Render("Tasks") + " " + m.styles.muted.Render(summary)
	return ansi.Truncate(line, m.width, "…")
}

func (m Model) footerView() string {
	hints := m.help.ShortHelpView(m.keys.help(m.focus))
	if m.focus == focusFilter {
		return ansi.Truncate(m.filter.View()+"  "+hints, m.width, "…")
	}
	return hints
}

func (m Model) listView(width, height int) string {
	if len(m.visible) == 0 {
		return m.styles.muted.Render("No matching tasks.")
	}

	first := max(m.cursor-height+1, 0)
	last := min(first+height, len(m.visible))
	rows := make([]string, 0, last-first)
	for pos := first; pos < last; pos++ {
		t := m.tasks[m.visible[pos]]
		style := m.styles.row
		switch {
		case pos == m.cursor:
			style = m.styles.cursorRow
		case t.Status == task.StatusDone:
			style = m.styles.doneRow
		}
		row := fmt.Sprintf("%s %-9s %s", statusIcon(t.Status), t.ID, t.Title)
		rows = append(rows, style.Width(width).Render(ansi.Truncate(row, width, "…")))
	}
	return strings.Join(rows, "\n")
}

func (m Model) renderDetails(t task.Task, width int) string {
	s := m.styles
	priority, ok := s.priority[t.Priority]
	if !ok {
		priority = s.row
	}

	due := s.muted.Render("none")
	if !t.Due.IsZero() {
		due = t.Due.Format("Mon, 02 Jan 2006")
		if t.Overdue(m.now) {
			due = s.overdue.Render(due + " (overdue)")
		}
	}

	tags := make([]string, 0, len(t.Tags))
	for _, tag := range t.Tags {
		tags = append(tags, s.tag.Render("#"+tag))
	}

	fields := []struct{ name, value string }{
		{"ID", t.ID},
		{"Status", statusIcon(t.Status) + " " + strings.ReplaceAll(string(t.Status), "_", " ")},
		{"Priority", priority.Render(string(t.Priority))},
		{"Due", due},
		{"Tags", strings.Join(tags, " ")},
	}
	lines := make([]string, 0, len(fields)+2)
	lines = append(lines, s.title.Render(t.Title))
	for _, f := range fields {
		lines = append(lines, s.label.Render(f.name)+f.value)
	}
	if t.Description != "" {
		lines = append(lines, "", t.Description)
	}
	return lipgloss.NewStyle().Width(width).Render(strings.Join(lines, "\n"))
}

func statusIcon(s task.Status) string {
	switch s {
	case task.StatusDone:
		return "●"
	case task.StatusInProgress:
		return "◐"
	default:
		return "○"
	}
}
