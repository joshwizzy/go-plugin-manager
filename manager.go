package manager

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	goplugin "github.com/hashicorp/go-plugin"
	"gopkg.in/tomb.v2"
)

type PluginInstance[T any] struct {
	Impl         T
	Cleanup      func()
	RestartCount int
}

type ManagerConfig struct {
	HandshakeConfig goplugin.HandshakeConfig
	PluginMap       goplugin.PluginSet
	RestartConfig   RestartConfig
	Logger          hclog.Logger
}
type RestartConfig struct {
	Managed         bool
	RestartNotifyCh chan PluginInfo
	PingInterval    time.Duration
	MaxRestarts     int
}

type Manager[C any] struct {
	sync.RWMutex
	config         *ManagerConfig
	plugins        map[string]PluginInstance[C]
	supervisorChan chan PluginInfo
	t              tomb.Tomb
}

func NewManager[C any](config *ManagerConfig) *Manager[C] {
	if config.RestartConfig.MaxRestarts == 0 {
		config.RestartConfig.MaxRestarts = 5
	}
	if config.RestartConfig.PingInterval == 0 {
		config.RestartConfig.PingInterval = 10 * time.Second
	}
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "plugin-manager",
			Output: os.Stdout,
			Level:  hclog.Debug,
		})
	}

	return &Manager[C]{
		config:         config,
		plugins:        make(map[string]PluginInstance[C]),
		supervisorChan: make(chan PluginInfo),
	}
}

type PluginInfo struct {
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

func (m *Manager[C]) loadPlugin(pm PluginInfo) (PluginInstance[C], error) {
	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: m.config.HandshakeConfig,
		Plugins:         m.config.PluginMap,
		Cmd:             exec.Command(pm.BinPath),
	})

	rpcClient, err := client.Client()
	if err != nil {
		m.config.Logger.Error(err.Error())
		return PluginInstance[C]{}, err
	}

	name := pluginKeys(m.config.PluginMap)[0]
	raw, err := rpcClient.Dispense(name)
	if err != nil {
		m.config.Logger.Error(err.Error())
		return PluginInstance[C]{}, err
	}

	impl, ok := raw.(C)
	if !ok {
		return PluginInstance[C]{}, fmt.Errorf("plugin does not implement interface")
	}
	supervisorChan := m.supervisorChan
	if !m.config.RestartConfig.Managed && m.config.RestartConfig.RestartNotifyCh != nil {
		supervisorChan = m.config.RestartConfig.RestartNotifyCh
	}
	watch := m.watchPlugin(m.config.RestartConfig.PingInterval, rpcClient, pm, supervisorChan)
	m.t.Go(watch)
	p := PluginInstance[C]{
		Impl:         impl,
		Cleanup:      client.Kill,
		RestartCount: 0,
	}
	return p, nil
}

func (m *Manager[C]) watchPlugin(interval time.Duration, rpcClient goplugin.ClientProtocol, pm PluginInfo, supervisorChan chan PluginInfo) func() error {
	ticker := time.NewTicker(interval)
	f := func() error {
		for {
			select {
			case <-ticker.C:
				if err := rpcClient.Ping(); err != nil {
					m.config.Logger.Debug("plugin %s exited will restart\n", pm.PluginKey)
					// Non-blocking send or discard
					select {
					case supervisorChan <- pm:
						// message sent
					default:
						// message dropped
					}
					return err
				}
			case <-m.t.Dying():
				return nil
			}
		}
	}
	return f
}

func (m *Manager[C]) LoadPlugins(plugins []PluginInfo) error {
	for _, pm := range plugins {
		p, err := m.loadPlugin(pm)
		if err != nil {
			return err
		}
		m.plugins[pm.PluginKey] = p
	}
	m.t.Go(m.monitorPlugins)
	return nil
}

func (m *Manager[C]) RestartPlugin(pm PluginInfo) {
	m.config.Logger.Debug("restarting plugin %s\n", pm.PluginKey)
	restartCount := 0
	if p, ok := m.GetPlugin(pm.PluginKey); ok {
		restartCount = p.RestartCount
		if restartCount >= m.config.RestartConfig.MaxRestarts {
			m.config.Logger.Debug("max restarts exceeded: %v\n", m.config.RestartConfig.MaxRestarts)
			return
		}
		if p.Cleanup != nil {
			p.Cleanup()
		}
	}

	err := m.deletePlugin(pm.PluginKey)
	if err != nil {
		m.config.Logger.Error("failed to delete plugin: %v", err)
		return
	}
	log.Printf("loading %v %v...", pm.PluginKey, pm.BinPath)
	p, err := m.loadPlugin(pm)
	if err != nil {
		m.config.Logger.Error("failed to load plugin: %v", err)
		return
	}
	p.RestartCount = restartCount + 1

	err = m.insertPlugin(pm.PluginKey, p)
	if err != nil {
		m.config.Logger.Error("failed to insert", err)
	}

	m.config.Logger.Debug("restarted plugin: %v", pm)
}

func (m *Manager[C]) monitorPlugins() error {
	for {
		select {
		case pm := <-m.supervisorChan:
			m.RestartPlugin(pm)

		case <-m.t.Dying():
			return nil
		}
	}
}

func (m *Manager[C]) Close() error {

	for _, client := range m.plugins {
		client.Cleanup()
	}
	m.t.Kill(nil)
	return m.t.Wait()
}

func (m *Manager[C]) GetPlugin(pluginKey string) (PluginInstance[C], bool) {
	m.Lock()
	defer m.Unlock()
	p, ok := m.plugins[pluginKey]
	return p, ok
}

func (m *Manager[C]) insertPlugin(pluginKey string, p PluginInstance[C]) error {
	m.Lock()
	defer m.Unlock()
	m.plugins[pluginKey] = p
	return nil
}

func (m *Manager[C]) deletePlugin(pluginKey string) error {
	m.Lock()
	defer m.Unlock()
	p, ok := m.plugins[pluginKey]
	if !ok {
		err := fmt.Errorf("plugin %v not found", pluginKey)
		m.config.Logger.Error(err.Error())
		return err
	}
	if p.Cleanup != nil {
		p.Cleanup()
	}
	return nil
}
