package ui

import "charm.land/bubbles/v2/key"

type keyMap struct {
	Up        key.Binding
	Down      key.Binding
	Switch    key.Binding
	Back      key.Binding
	Toggle    key.Binding
	Filter    key.Binding
	Apply     key.Binding
	Clear     key.Binding
	Quit      key.Binding
	ForceQuit key.Binding
}

func newKeyMap() keyMap {
	return keyMap{
		Up:        key.NewBinding(key.WithKeys("k", "up"), key.WithHelp("k/↑", "up")),
		Down:      key.NewBinding(key.WithKeys("j", "down"), key.WithHelp("j/↓", "down")),
		Switch:    key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "switch pane")),
		Back:      key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "back")),
		Toggle:    key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "toggle done")),
		Filter:    key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "filter")),
		Apply:     key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "apply")),
		Clear:     key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "clear")),
		Quit:      key.NewBinding(key.WithKeys("q"), key.WithHelp("q", "quit")),
		ForceQuit: key.NewBinding(key.WithKeys("ctrl+c"), key.WithHelp("ctrl+c", "quit")),
	}
}

func (k keyMap) help(f focus) []key.Binding {
	switch f {
	case focusFilter:
		return []key.Binding{k.Apply, k.Clear, k.ForceQuit}
	case focusDetails:
		return []key.Binding{k.Up, k.Down, k.Switch, k.Back, k.Toggle, k.Filter, k.Quit}
	default:
		return []key.Binding{k.Up, k.Down, k.Switch, k.Toggle, k.Filter, k.Clear, k.Quit}
	}
}
