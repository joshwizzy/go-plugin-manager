package manager

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"time"

	goplugin "github.com/hashicorp/go-plugin"
)

type loadedPlugin[T any] struct {
	Client  T
	Cleanup func()
}

type Manager[C any] struct {
	sync.RWMutex
	plugins        map[string]loadedPlugin[C]
	supervisorChan chan PluginMetaData
	pluginMap      map[string]goplugin.Plugin
}

func NewManager[C any]() *Manager[C] {
	return &Manager[C]{
		plugins:        make(map[string]loadedPlugin[C]),
		supervisorChan: make(chan PluginMetaData),
	}
}

type PluginMetaData struct {
	BinPath   string
	PluginKey string
}

func (m *Manager[C]) LoadPlugin(ctx context.Context, pm PluginMetaData) (loadedPlugin[C], error) {
	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins:         m.pluginMap,
		Cmd:             exec.Command(pm.BinPath),
	})

	rpcClient, err := client.Client()
	if err != nil {
		log.Println(err)
		return loadedPlugin[C]{}, err
	}

	raw, err := rpcClient.Dispense("ussd")
	if err != nil {
		log.Println(err)
		return loadedPlugin[C]{}, err
	}

	impl, ok := raw.(C)
	if !ok {
		return loadedPlugin[C]{}, fmt.Errorf("plugin does not implement interface")
	}
	go watchPlugin(ctx, pm, rpcClient, m.supervisorChan)
	return loadedPlugin[C]{impl, client.Kill}, nil
}

func watchPlugin(ctx context.Context, pm PluginMetaData, rpcClient goplugin.ClientProtocol, supervisorChan chan PluginMetaData) {
	interval := 10 * time.Second
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if err := rpcClient.Ping(); err != nil {
					log.Printf("plugin %s exited will restart\n", pm.PluginKey)
					supervisorChan <- pm
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (m *Manager[C]) LoadPlugins(ctx context.Context, pluginMap map[string]goplugin.Plugin, plugins []PluginMetaData) error {
	m.pluginMap = pluginMap
	for _, pm := range plugins {
		p, err := m.LoadPlugin(ctx, pm)
		if err != nil {
			return err
		}
		m.plugins[pm.PluginKey] = p
	}
	go m.HealthMonitor(ctx)
	return nil

}

func (m *Manager[C]) HealthMonitor(ctx context.Context) {
	for {
		select {
		case pm := <-m.supervisorChan:
			log.Printf("restarting plugin %s\n", pm.PluginKey)
			if p, ok := m.Get(pm.PluginKey); ok && p.Cleanup != nil {
				log.Println("cleaning up...")
				p.Cleanup()
			}
			log.Println("deleting..")
			m.Del(pm.PluginKey)
			log.Println("loading...")
			p, err := m.LoadPlugin(ctx, pm)
			if err != nil {
				log.Println("inserting...")
				m.Insert(pm.PluginKey, p)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager[C]) Close() {

	for _, client := range m.plugins {
		client.Cleanup()
	}
}

func (m *Manager[C]) Get(pluginKey string) (loadedPlugin[C], bool) {
	m.Lock()
	defer m.Unlock()
	p, ok := m.plugins[pluginKey]
	return p, ok
}

func (m *Manager[C]) Insert(pluginKey string, p loadedPlugin[C]) error {
	m.Lock()
	defer m.Unlock()
	m.plugins[pluginKey] = p
	return nil
}

func (m *Manager[C]) Del(pluginKey string) error {
	m.Lock()
	defer m.Unlock()
	p, ok := m.plugins[pluginKey]
	if !ok {
		return fmt.Errorf("plugin %v not found", pluginKey)
	}
	if p.Cleanup != nil {
		p.Cleanup()
	}
	return nil
}
