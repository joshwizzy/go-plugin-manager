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
	Impl    T
	Cleanup func()
}

type ManagerConfig struct {
	HandshakeConfig goplugin.HandshakeConfig
	PluginMap       goplugin.PluginSet
	RestartConfig   RestartConfig
}
type RestartConfig struct {
	Managed         bool
	RestartNotifyCh chan PluginMetaData
	PingInterval    time.Duration
	MaxRestarts     int
}

type Manager[C any] struct {
	sync.RWMutex
	config         *ManagerConfig
	plugins        map[string]loadedPlugin[C]
	supervisorChan chan PluginMetaData
}

func NewManager[C any](config *ManagerConfig) *Manager[C] {
	if config.RestartConfig.MaxRestarts == 0 {
		config.RestartConfig.MaxRestarts = 5
	}
	if config.RestartConfig.PingInterval == 0 {
		config.RestartConfig.PingInterval = 10 * time.Second
	}

	return &Manager[C]{
		config:         config,
		plugins:        make(map[string]loadedPlugin[C]),
		supervisorChan: make(chan PluginMetaData),
	}
}

type PluginMetaData struct {
	BinPath   string
	PluginKey string
}

func pluginKeys(ps goplugin.PluginSet) []string {
	keys := []string{}
	for key, _ := range ps {
		keys = append(keys, key)
	}
	return keys
}

func (m *Manager[C]) LoadPlugin(ctx context.Context, pm PluginMetaData) (loadedPlugin[C], error) {
	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: m.config.HandshakeConfig,
		Plugins:         m.config.PluginMap,
		Cmd:             exec.Command(pm.BinPath),
	})

	rpcClient, err := client.Client()
	if err != nil {
		log.Println(err)
		return loadedPlugin[C]{}, err
	}

	name := pluginKeys(m.config.PluginMap)[0]
	raw, err := rpcClient.Dispense(name)
	if err != nil {
		log.Println(err)
		return loadedPlugin[C]{}, err
	}

	impl, ok := raw.(C)
	if !ok {
		return loadedPlugin[C]{}, fmt.Errorf("plugin does not implement interface")
	}
	supervisorChan := m.supervisorChan
	if !m.config.RestartConfig.Managed && m.config.RestartConfig.RestartNotifyCh != nil {
		supervisorChan = m.config.RestartConfig.RestartNotifyCh
	}
	watchPlugin(ctx, pm, rpcClient, supervisorChan, m.config.RestartConfig.PingInterval)
	return loadedPlugin[C]{impl, client.Kill}, nil
}

func watchPlugin(ctx context.Context, pm PluginMetaData, rpcClient goplugin.ClientProtocol, supervisorChan chan PluginMetaData, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if err := rpcClient.Ping(); err != nil {
					log.Printf("plugin %s exited will restart\n", pm.PluginKey)
					// Non-blocking send or discard
					select {
					case supervisorChan <- pm:
						// message sent
					default:
						// message dropped
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (m *Manager[C]) LoadPlugins(ctx context.Context, plugins []PluginMetaData) error {
	for _, pm := range plugins {
		p, err := m.LoadPlugin(ctx, pm)
		if err != nil {
			return err
		}
		m.plugins[pm.PluginKey] = p
	}
	go m.monitorPlugins(ctx)
	return nil
}

func (m *Manager[C]) RestartPlugin(ctx context.Context, pm PluginMetaData) {
	log.Printf("restarting plugin %s\n", pm.PluginKey)
	if p, ok := m.Get(pm.PluginKey); ok && p.Cleanup != nil {
		log.Println("cleaning up...")
		p.Cleanup()
	}
	log.Println("deleting..")
	err := m.Del(pm.PluginKey)
	if err != nil {
		log.Println("failed to delete", err)
		return
	}
	log.Printf("loading %v %v...", pm.PluginKey, pm.BinPath)
	p, err := m.LoadPlugin(ctx, pm)
	if err != nil {
		log.Println("failed to load", err)
		return
	}
	log.Println("inserting...")
	err = m.Insert(pm.PluginKey, p)
	if err != nil {
		log.Println("failed to insert", err)
	}
}

func (m *Manager[C]) monitorPlugins(ctx context.Context) {
	for {
		select {
		case pm := <-m.supervisorChan:
			m.RestartPlugin(ctx, pm)

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
