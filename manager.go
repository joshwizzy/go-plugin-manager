package sdk

import (
	"fmt"
	"os/exec"

	goplugin "github.com/hashicorp/go-plugin"
)

type Manager[C any] struct {
	handlers      map[string]C
	pluginClients []*goplugin.Client
}

func NewManager[C any]() *Manager[C] {
	return &Manager[C]{handlers: make(map[string]C)}
}

type PluginMetaData struct {
	binPath   string
	pluginKey string
}

func (m *Manager[C]) LoadPlugins(pluginMap map[string]goplugin.Plugin, plugins []PluginMetaData) error {
	for _, meta := range plugins {
		client := goplugin.NewClient(&goplugin.ClientConfig{
			HandshakeConfig: Handshake,
			Plugins:         pluginMap,
			Cmd:             exec.Command(meta.binPath),
		})

		m.pluginClients = append(m.pluginClients, client)

		rpcClient, err := client.Client()
		if err != nil {
			return err
		}

		raw, err := rpcClient.Dispense("ussd")
		if err != nil {
			return err
		}

		impl, ok := raw.(C)
		if !ok {
			return fmt.Errorf("plugin does not implement interface")
		}

		m.handlers[meta.pluginKey] = impl
	}
	return nil
}

func (m *Manager[C]) Close() {
	for _, client := range m.pluginClients {
		client.Kill()
	}
}

func (m *Manager[C]) Get(pluginKey string) (C, bool) {
	h, ok := m.handlers[pluginKey]
	return h, ok
}
