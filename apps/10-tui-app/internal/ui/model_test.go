package ui

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/10-tui-app/internal/task"
)

var testNow = time.Date(2026, time.September, 21, 10, 0, 0, 0, time.UTC)

func sampleTasks() []task.Task {
	return []task.Task{
		{
			ID: "API-1", Title: "Paginate invoices", Status: task.StatusTodo, Priority: task.PriorityHigh,
			Tags: []string{"api", "billing"}, Due: task.Date{Time: time.Date(2026, time.September, 18, 0, 0, 0, 0, time.UTC)},
		},
		{
			ID: "OPS-2", Title: "Write runbook", Status: task.StatusInProgress, Priority: task.PriorityMedium,
			Tags:        []string{"oncall"},
			Description: strings.Repeat("Document the replay endpoint and the paging policy. ", 20),
		},
		{
			ID: "SEC-3", Title: "Remove static keys", Status: task.StatusDone, Priority: task.PriorityHigh,
			Tags: []string{"security", "ci"},
		},
		{
			ID: "WEB-4", Title: "Fix report timezone", Status: task.StatusTodo, Priority: task.PriorityLow,
			Tags: []string{"bug"},
		},
	}
}

func press(k string) tea.KeyPressMsg {
	switch k {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	case "down":
		return tea.KeyPressMsg{Code: tea.KeyDown}
	case "ctrl+c":
		return tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}
	default:
		return tea.KeyPressMsg{Code: []rune(k)[0], Text: k}
	}
}

func keys(ks ...string) []tea.Msg {
	msgs := make([]tea.Msg, 0, len(ks))
	for _, k := range ks {
		msgs = append(msgs, press(k))
	}
	return msgs
}

func typed(s string) []tea.Msg {
	msgs := make([]tea.Msg, 0, len(s))
	for _, r := range s {
		msgs = append(msgs, press(string(r)))
	}
	return msgs
}

func seq(parts ...[]tea.Msg) []tea.Msg {
	var msgs []tea.Msg
	for _, p := range parts {
		msgs = append(msgs, p...)
	}
	return msgs
}

func send(m Model, msgs ...tea.Msg) (Model, tea.Cmd) {
	var cmd tea.Cmd
	var next tea.Model = m
	for _, msg := range msgs {
		next, cmd = next.Update(msg)
	}
	return next.(Model), cmd
}

func isQuit(cmd tea.Cmd) bool {
	if cmd == nil {
		return false
	}
	_, ok := cmd().(tea.QuitMsg)
	return ok
}

func selectedID(m Model) string {
	i, ok := m.selectedIndex()
	if !ok {
		return ""
	}
	return m.tasks[i].ID
}

func visibleIDs(m Model) []string {
	ids := make([]string, 0, len(m.visible))
	for _, i := range m.visible {
		ids = append(ids, m.tasks[i].ID)
	}
	return ids
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	all := []string{"API-1", "OPS-2", "SEC-3", "WEB-4"}
	tests := []struct {
		name         string
		msgs         []tea.Msg
		wantSelected string
		wantVisible  []string
		wantFocus    focus
		wantFilter   string
		wantQuit     bool
	}{
		{name: "initial state", wantSelected: "API-1", wantVisible: all},
		{name: "j moves down", msgs: keys("j"), wantSelected: "OPS-2", wantVisible: all},
		{name: "k stops at the top", msgs: keys("k", "k"), wantSelected: "API-1", wantVisible: all},
		{name: "j stops at the bottom", msgs: typed("jjjjjj"), wantSelected: "WEB-4", wantVisible: all},
		{name: "arrow keys move", msgs: keys("down", "down", "up"), wantSelected: "OPS-2", wantVisible: all},
		{name: "enter focuses details", msgs: keys("enter"), wantSelected: "API-1", wantVisible: all, wantFocus: focusDetails},
		{name: "enter toggles back to the list", msgs: keys("enter", "enter"), wantSelected: "API-1", wantVisible: all},
		{name: "esc leaves details", msgs: keys("enter", "esc"), wantSelected: "API-1", wantVisible: all},
		{
			name:         "j in details scrolls instead of moving",
			msgs:         keys("j", "enter", "j", "j"),
			wantSelected: "OPS-2",
			wantVisible:  all,
			wantFocus:    focusDetails,
		},
		{name: "q quits", msgs: keys("q"), wantSelected: "API-1", wantVisible: all, wantQuit: true},
		{name: "q quits from details", msgs: keys("enter", "q"), wantSelected: "API-1", wantVisible: all, wantFocus: focusDetails, wantQuit: true},
		{name: "slash enters filter mode", msgs: keys("/"), wantSelected: "API-1", wantVisible: all, wantFocus: focusFilter},
		{
			name:         "typing filters by title case-insensitively",
			msgs:         seq(keys("/"), typed("RUNBOOK")),
			wantSelected: "OPS-2",
			wantVisible:  []string{"OPS-2"},
			wantFocus:    focusFilter,
			wantFilter:   "RUNBOOK",
		},
		{
			name:         "typing filters by tag",
			msgs:         seq(keys("/"), typed("billing")),
			wantSelected: "API-1",
			wantVisible:  []string{"API-1"},
			wantFocus:    focusFilter,
			wantFilter:   "billing",
		},
		{
			name:        "q and j type into the filter",
			msgs:        seq(keys("/"), typed("qj")),
			wantVisible: []string{},
			wantFocus:   focusFilter,
			wantFilter:  "qj",
		},
		{
			name:         "ctrl+c quits from filter mode",
			msgs:         keys("/", "ctrl+c"),
			wantSelected: "API-1",
			wantVisible:  all,
			wantFocus:    focusFilter,
			wantQuit:     true,
		},
		{
			name:         "enter applies the filter",
			msgs:         seq(keys("/"), typed("bug"), keys("enter")),
			wantSelected: "WEB-4",
			wantVisible:  []string{"WEB-4"},
			wantFilter:   "bug",
		},
		{
			name:         "esc clears the filter",
			msgs:         seq(keys("/"), typed("bug"), keys("esc")),
			wantSelected: "WEB-4",
			wantVisible:  all,
		},
		{
			name:         "esc in the list clears an applied filter",
			msgs:         seq(keys("/"), typed("bug"), keys("enter", "esc")),
			wantSelected: "WEB-4",
			wantVisible:  all,
		},
		{
			name:         "selection survives filtering",
			msgs:         seq(typed("jj"), keys("/"), typed("re")),
			wantSelected: "SEC-3",
			wantVisible:  []string{"SEC-3", "WEB-4"},
			wantFocus:    focusFilter,
			wantFilter:   "re",
		},
		{
			name:         "selection falls back to the first match",
			msgs:         seq(typed("j"), keys("/"), typed("re")),
			wantSelected: "SEC-3",
			wantVisible:  []string{"SEC-3", "WEB-4"},
			wantFocus:    focusFilter,
			wantFilter:   "re",
		},
		{
			name:         "window size does not change selection",
			msgs:         []tea.Msg{press("j"), tea.WindowSizeMsg{Width: 100, Height: 30}},
			wantSelected: "OPS-2",
			wantVisible:  all,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, cmd := send(New(sampleTasks(), testNow), tt.msgs...)
			require.Equal(t, tt.wantSelected, selectedID(m))
			require.Equal(t, tt.wantVisible, visibleIDs(m))
			require.Equal(t, tt.wantFocus, m.focus)
			require.Equal(t, tt.wantFilter, m.filter.Value())
			require.Equal(t, tt.wantQuit, isQuit(cmd))
		})
	}
}

func TestUpdateToggleDone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		msgs         []tea.Msg
		wantID       string
		wantStatus   task.Status
		wantModified bool
	}{
		{name: "todo becomes done", msgs: keys("x"), wantID: "API-1", wantStatus: task.StatusDone, wantModified: true},
		{name: "done becomes todo", msgs: typed("jjx"), wantID: "SEC-3", wantStatus: task.StatusTodo, wantModified: true},
		{name: "in progress becomes done", msgs: typed("jx"), wantID: "OPS-2", wantStatus: task.StatusDone, wantModified: true},
		{name: "toggle twice restores status", msgs: typed("xx"), wantID: "API-1", wantStatus: task.StatusTodo, wantModified: true},
		{name: "works from details", msgs: keys("enter", "x"), wantID: "API-1", wantStatus: task.StatusDone, wantModified: true},
		{name: "x types into the filter", msgs: keys("/", "x"), wantID: "API-1", wantStatus: task.StatusTodo},
		{
			name:       "no selection is a no-op",
			msgs:       seq(keys("/"), typed("nothing"), keys("enter", "x")),
			wantID:     "API-1",
			wantStatus: task.StatusTodo,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input := sampleTasks()
			initial := New(input, testNow)
			m, cmd := send(initial, tt.msgs...)

			got := findTask(t, m.Tasks(), tt.wantID)
			require.Equal(t, tt.wantStatus, got.Status)
			require.Equal(t, tt.wantModified, m.Modified())
			require.Nil(t, cmd)

			require.Equal(t, sampleTasks(), input)
			require.Equal(t, sampleTasks(), initial.Tasks())
			require.False(t, initial.Modified())
		})
	}
}

func findTask(t *testing.T, tasks []task.Task, id string) task.Task {
	t.Helper()
	for _, tk := range tasks {
		if tk.ID == id {
			return tk
		}
	}
	t.Fatalf("task %s not found", id)
	return task.Task{}
}

func TestUpdateWindowSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		width, height int
		wantDetailsW  int
		wantDetailsH  int
	}{
		{name: "wide", width: 120, height: 30, wantDetailsW: 76, wantDetailsH: 26},
		{name: "list keeps its minimum width", width: 60, height: 20, wantDetailsW: 28, wantDetailsH: 16},
		{name: "narrower than the list", width: 20, height: 3, wantDetailsW: 0, wantDetailsH: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, cmd := send(New(sampleTasks(), testNow), tea.WindowSizeMsg{Width: tt.width, Height: tt.height})
			require.Nil(t, cmd)
			require.Equal(t, tt.wantDetailsW, m.details.Width())
			require.Equal(t, tt.wantDetailsH, m.details.Height())
		})
	}
}

func TestUpdateDetailsScroll(t *testing.T) {
	t.Parallel()

	size := tea.WindowSizeMsg{Width: 80, Height: 12}
	tests := []struct {
		name       string
		msgs       []tea.Msg
		wantOffset int
	}{
		{name: "j scrolls down", msgs: seq(keys("j", "enter"), typed("jj")), wantOffset: 2},
		{name: "k stops at the top", msgs: seq(keys("j", "enter"), typed("jk"), keys("k")), wantOffset: 0},
		{name: "list j does not scroll", msgs: typed("jj"), wantOffset: 0},
		{name: "changing selection resets scroll", msgs: seq(keys("j", "enter"), typed("jj"), keys("enter", "j")), wantOffset: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, _ := send(New(sampleTasks(), testNow), append([]tea.Msg{size}, tt.msgs...)...)
			require.Equal(t, tt.wantOffset, m.details.YOffset())
		})
	}
}

func TestView(t *testing.T) {
	t.Parallel()

	size := tea.WindowSizeMsg{Width: 100, Height: 20}
	tests := []struct {
		name    string
		msgs    []tea.Msg
		want    []string
		notWant []string
	}{
		{
			name: "list and details",
			msgs: []tea.Msg{size},
			want: []string{
				"Tasks", "4 of 4 shown · 1 done",
				"○ API-1", "◐ OPS-2", "● SEC-3", "Paginate invoices", "Write runbook",
				"Priority", "high", "#billing", "Fri, 18 Sep 2026 (overdue)",
				"/ filter", "q quit",
			},
		},
		{
			name: "details follow the cursor",
			msgs: []tea.Msg{size, press("j")},
			want: []string{"in progress", "Document the replay endpoint"},
		},
		{
			name:    "filter mode shows the input",
			msgs:    seq([]tea.Msg{size}, keys("/"), typed("bil")),
			want:    []string{"/bil", "1 of 4 shown", "enter apply", "esc clear"},
			notWant: []string{"Write runbook", "q quit"},
		},
		{
			name: "applied filter is shown in the header",
			msgs: seq([]tea.Msg{size}, keys("/"), typed("bug"), keys("enter")),
			want: []string{`filter "bug"`, "Fix report timezone"},
		},
		{
			name: "no matches",
			msgs: seq([]tea.Msg{size}, keys("/"), typed("zzz")),
			want: []string{"No matching tasks.", "No task selected."},
		},
		{
			name:    "list scrolls to keep the cursor visible",
			msgs:    seq([]tea.Msg{tea.WindowSizeMsg{Width: 100, Height: 6}}, typed("jjj")),
			want:    []string{"● SEC-3", "○ WEB-4"},
			notWant: []string{"API-1", "OPS-2"},
		},
		{
			name: "unsaved changes",
			msgs: []tea.Msg{size, press("x")},
			want: []string{"2 done", "unsaved"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, _ := send(New(sampleTasks(), testNow), tt.msgs...)
			v := m.View()
			require.True(t, v.AltScreen)
			content := ansi.Strip(v.Content)
			for _, s := range tt.want {
				require.Contains(t, content, s)
			}
			for _, s := range tt.notWant {
				require.NotContains(t, content, s)
			}
		})
	}
}

func TestViewBeforeWindowSize(t *testing.T) {
	t.Parallel()

	require.Empty(t, New(sampleTasks(), testNow).View().Content)
}
