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
	Kill         func()
	RestartCount int
}

type PluginInfo struct {
	BinPath   string
	PluginKey string
}

type ManagerConfig struct {
	HandshakeConfig goplugin.HandshakeConfig
	PluginMap       goplugin.PluginSet
	RestartConfig   RestartConfig
	Logger          hclog.Logger
}
type RestartConfig struct {
	Managed      bool
	PingInterval time.Duration
	MaxRestarts  int
}

type Manager[C any] struct {
	sync.RWMutex
	Name    string
	c       chan PluginInfo
	config  *ManagerConfig
	plugins map[string]PluginInstance[C]
	t       tomb.Tomb
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

	c := make(chan PluginInfo)
	m := &Manager[C]{
		Name:    pluginKey(config.PluginMap),
		config:  config,
		plugins: make(map[string]PluginInstance[C]),
	}
	if config.RestartConfig.Managed {
		m.t.Go(m.startPluginWatcher(c))

	} else {
		m.c = c
	}

	return m
}

func pluginKey(ps goplugin.PluginSet) (key string) {
	for k := range ps {
		key = k
		break
	}
	return
}

func (m *Manager[C]) Ch() <-chan PluginInfo {
	if m.config.RestartConfig.Managed {
		panic("plugin manager is managed")
	}
	return m.c
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

	raw, err := rpcClient.Dispense(m.Name)
	if err != nil {
		m.config.Logger.Error(err.Error())
		return PluginInstance[C]{}, err
	}

	impl, ok := raw.(C)
	if !ok {
		return PluginInstance[C]{}, fmt.Errorf("plugin does not implement interface")
	}

	watcher := m.pluginWatcher(m.config.RestartConfig.PingInterval, rpcClient, pm)
	m.t.Go(watcher)
	p := PluginInstance[C]{
		Impl:         impl,
		Kill:         client.Kill,
		RestartCount: 0,
	}
	return p, nil
}

func (m *Manager[C]) pluginWatcher(
	interval time.Duration,
	rpcClient goplugin.ClientProtocol,
	pm PluginInfo,
) func() error {
	ticker := time.NewTicker(interval)
	f := func() error {
		for {
			select {
			case <-ticker.C:
				if err := rpcClient.Ping(); err != nil {
					m.config.Logger.Debug("plugin %s exited will restart\n", pm.PluginKey)
					// Non-blocking send or discard
					select {
					case m.c <- pm:
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
		if p.Kill != nil {
			p.Kill()
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

func (m *Manager[C]) startPluginWatcher(c <-chan PluginInfo) func() error {
	return func() error {
		for {
			select {
			case pm := <-c:
				m.RestartPlugin(pm)

			case <-m.t.Dying():
				return nil
			}
		}
	}
}

func (m *Manager[C]) Close() error {

	for _, client := range m.plugins {
		client.Kill()
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
	if p.Kill != nil {
		p.Kill()
	}
	return nil
}
