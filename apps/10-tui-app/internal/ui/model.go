// Package ui implements the interactive task browser as a Bubble Tea model.
package ui

import (
	"slices"
	"strings"
	"time"

	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/textinput"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/Maksim-Burtsev/go-reading/apps/10-tui-app/internal/task"
)

const (
	minListWidth = 28
	chromeHeight = 2
	filterWidth  = 32
)

type focus int

const (
	focusList focus = iota
	focusDetails
	focusFilter
)

// Model is the state of the task browser.
type Model struct {
	tasks    []task.Task
	visible  []int
	cursor   int
	focus    focus
	modified bool
	now      time.Time

	filter  textinput.Model
	details viewport.Model
	help    help.Model
	keys    keyMap
	styles  styles

	width  int
	height int
}

// New returns a browser over tasks. now is used to flag overdue tasks.
func New(tasks []task.Task, now time.Time) Model {
	filter := textinput.New()
	filter.Prompt = "/"
	filter.Placeholder = "title or tag"
	filter.SetWidth(filterWidth)
	inputStyles := textinput.DefaultDarkStyles()
	inputStyles.Cursor.Blink = false
	filter.SetStyles(inputStyles)

	m := Model{
		tasks:   tasks,
		now:     now,
		filter:  filter,
		details: viewport.New(),
		help:    help.New(),
		keys:    newKeyMap(),
		styles:  newStyles(),
	}
	m.applyFilter()
	return m
}

// Tasks returns the tasks, including any status changes made in the browser.
func (m Model) Tasks() []task.Task {
	return m.tasks
}

// Modified reports whether any task was changed in the browser.
func (m Model) Modified() bool {
	return m.modified
}

// Init implements tea.Model.
func (m Model) Init() tea.Cmd {
	return nil
}

// Update implements tea.Model.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if size, ok := msg.(tea.WindowSizeMsg); ok {
		m.resize(size.Width, size.Height)
		return m, nil
	}

	keyMsg, isKey := msg.(tea.KeyPressMsg)
	if isKey && key.Matches(keyMsg, m.keys.ForceQuit) {
		return m, tea.Quit
	}

	switch {
	case m.focus == focusFilter:
		return m.updateFilter(msg)
	case isKey:
		return m.updateBrowse(keyMsg)
	default:
		return m, nil
	}
}

func (m Model) updateBrowse(msg tea.KeyPressMsg) (Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Quit):
		return m, tea.Quit
	case key.Matches(msg, m.keys.Filter):
		m.focus = focusFilter
		cmd := m.filter.Focus()
		return m, cmd
	case key.Matches(msg, m.keys.Toggle):
		m.toggleDone()
	case key.Matches(msg, m.keys.Switch):
		if m.focus == focusList {
			m.focus = focusDetails
		} else {
			m.focus = focusList
		}
	case m.focus == focusDetails && key.Matches(msg, m.keys.Back):
		m.focus = focusList
	case m.focus == focusDetails:
		var cmd tea.Cmd
		m.details, cmd = m.details.Update(msg)
		return m, cmd
	case key.Matches(msg, m.keys.Clear):
		m.filter.Reset()
		m.applyFilter()
	case key.Matches(msg, m.keys.Up):
		m.moveCursor(-1)
	case key.Matches(msg, m.keys.Down):
		m.moveCursor(1)
	}
	return m, nil
}

func (m Model) updateFilter(msg tea.Msg) (Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyPressMsg); ok {
		switch {
		case key.Matches(keyMsg, m.keys.Apply):
			m.filter.Blur()
			m.focus = focusList
			return m, nil
		case key.Matches(keyMsg, m.keys.Clear):
			m.filter.Blur()
			m.filter.Reset()
			m.focus = focusList
			m.applyFilter()
			return m, nil
		}
	}

	var cmd tea.Cmd
	m.filter, cmd = m.filter.Update(msg)
	m.applyFilter()
	return m, cmd
}

func (m *Model) resize(width, height int) {
	m.width, m.height = width, height
	m.help.SetWidth(width)

	_, detailsWidth := m.paneWidths()
	frameX, frameY := m.styles.pane.GetFrameSize()
	m.details.SetWidth(max(detailsWidth-frameX, 0))
	m.details.SetHeight(max(m.bodyHeight()-frameY, 0))
	m.syncDetails()
}

func (m Model) paneWidths() (list, details int) {
	list = min(max(m.width/3, minListWidth), m.width)
	return list, m.width - list
}

func (m Model) bodyHeight() int {
	return max(m.height-chromeHeight, 0)
}

func (m Model) selectedIndex() (int, bool) {
	if m.cursor >= len(m.visible) {
		return 0, false
	}
	return m.visible[m.cursor], true
}

func (m *Model) moveCursor(delta int) {
	if len(m.visible) == 0 {
		return
	}
	next := min(max(m.cursor+delta, 0), len(m.visible)-1)
	if next == m.cursor {
		return
	}
	m.cursor = next
	m.syncDetails()
}

func (m *Model) applyFilter() {
	selected, hadSelection := m.selectedIndex()
	query := strings.TrimSpace(m.filter.Value())

	visible := make([]int, 0, len(m.tasks))
	for i, t := range m.tasks {
		if t.Matches(query) {
			visible = append(visible, i)
		}
	}
	m.visible = visible

	m.cursor = 0
	if hadSelection {
		m.cursor = max(slices.Index(visible, selected), 0)
	}
	m.syncDetails()
}

func (m *Model) toggleDone() {
	i, ok := m.selectedIndex()
	if !ok {
		return
	}
	m.tasks = slices.Clone(m.tasks)
	if m.tasks[i].Status == task.StatusDone {
		m.tasks[i].Status = task.StatusTodo
	} else {
		m.tasks[i].Status = task.StatusDone
	}
	m.modified = true
	m.syncDetails()
}

func (m *Model) syncDetails() {
	i, ok := m.selectedIndex()
	if !ok {
		m.details.SetContent(m.styles.muted.Render("No task selected."))
		return
	}
	m.details.SetContent(m.renderDetails(m.tasks[i], m.details.Width()))
	m.details.GotoTop()
}
