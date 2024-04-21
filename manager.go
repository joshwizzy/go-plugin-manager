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
)

type PluginInstance[T any] struct {
	Impl         T
	client       *goplugin.Client
	rpcClient    goplugin.ClientProtocol
	RestartCount int
	Info         PluginInfo
	stop         chan struct{}
	done         chan struct{}
}

func (p *PluginInstance[T]) Kill() {
	p.client.Kill()
}

func (p *PluginInstance[T]) Ping() error {
	return p.rpcClient.Ping()
}

type PluginInfo struct {
	BinPath string
	Key     string
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
	RestartFunc  func(l hclog.Logger, pi PluginInfo) error
}

type Manager[C any] struct {
	mu      sync.RWMutex
	Name    string
	killed  chan PluginInfo
	config  *ManagerConfig
	plugins map[string]*PluginInstance[C]
	stop    chan struct{}
	done    chan struct{}
	// t       tomb.Tomb
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

	killed := make(chan PluginInfo)
	m := &Manager[C]{
		Name:    pluginKey(config.PluginMap),
		config:  config,
		plugins: make(map[string]*PluginInstance[C]),
		killed:  killed,
		done:    make(chan struct{}),
		stop:    make(chan struct{}),
	}

	// m.t.Go(m.supervisorFunc(killed))
	go m.supervisor()
	return m
}

func (m *Manager[C]) Killed() <-chan PluginInfo {
	return m.killed
}

func pluginKey(ps goplugin.PluginSet) (key string) {
	for k := range ps {
		key = k
		break
	}
	return
}

func (m *Manager[C]) loadPlugin(pm PluginInfo) (*PluginInstance[C], error) {
	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: m.config.HandshakeConfig,
		Plugins:         m.config.PluginMap,
		Cmd:             exec.Command(pm.BinPath),
	})

	rpcClient, err := client.Client()
	if err != nil {
		m.config.Logger.Error(err.Error())
		return nil, err
	}

	raw, err := rpcClient.Dispense(m.Name)
	if err != nil {
		m.config.Logger.Error(err.Error())
		return nil, err
	}

	impl, ok := raw.(C)
	if !ok {
		return nil, fmt.Errorf("plugin does not implement interface")
	}

	stop, done := make(chan struct{}), make(chan struct{})
	p := &PluginInstance[C]{
		Impl:         impl,
		client:       client,
		rpcClient:    rpcClient,
		RestartCount: 0,
		stop:         stop,
		done:         done,
		Info:         pm,
	}
	go p.Watch(m.config.Logger, m.config.RestartConfig.PingInterval, m.killed)

	return p, nil
}

func (p *PluginInstance[C]) Watch(
	l hclog.Logger,
	interval time.Duration,
	killed chan PluginInfo,
) {
	defer close(p.done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			log.Println("we done")
			return
		case <-ticker.C:
			if err := p.Ping(); err != nil {
				l.Debug("plugin %s exited will restart\n", p.Info.Key)
				// if p, ok := m.GetPlugin(pm.Key); ok && p.Unloaded() {
				// 	return nil
				// }

				// Non-blocking send or discard
				select {
				case killed <- p.Info:
					// message sent
				default:
					// message dropped
				}
				return
			}
		case <-p.stop:
			return
		}
	}
}

func (m *Manager[C]) LoadPlugins(plugins []PluginInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, pm := range plugins {
		p, err := m.loadPlugin(pm)
		if err != nil {
			return err
		}
		m.plugins[pm.Key] = p
	}

	return nil
}

type PluginMetadata struct {
	Key      string `json:"key"`
	Restarts int    `json:"restarts"`
}

func (m *Manager[C]) ListPlugins() ([]PluginMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	metas := []PluginMetadata{}
	for key, p := range m.plugins {
		meta := PluginMetadata{
			Key:      key,
			Restarts: p.RestartCount,
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

func (m *Manager[c]) StopPlugin(pm PluginInfo) error {
	m.config.Logger.Debug("unloading plugin %s\n", pm.Key)

	p, ok := m.GetPlugin(pm.Key)
	if ok {
		p.Kill()

		close(p.done)
	}

	err := m.deletePlugin(pm.Key)
	if err != nil {
		m.config.Logger.Error("failed to delete plugin: %v", err)
		return err
	}

	return nil

}

func (m *Manager[C]) StartPlugin(pm PluginInfo) (*PluginInstance[C], error) {
	log.Printf("loading %v %v...", pm.Key, pm.BinPath)
	p, err := m.loadPlugin(pm)
	if err != nil {
		m.config.Logger.Error("failed to load plugin: %v", err)
		return nil, err
	}

	err = m.insertPlugin(pm.Key, p)
	if err != nil {
		m.config.Logger.Error("failed to insert", err)
		return nil, err
	}
	return p, nil

}

func (m *Manager[C]) RestartPlugin(pm PluginInfo) error {
	m.config.Logger.Debug("restarting plugin %s\n", pm.Key)
	restartCount := 0
	p, ok := m.GetPlugin(pm.Key)
	if ok {
		restartCount = p.RestartCount
		if restartCount >= m.config.RestartConfig.MaxRestarts {
			err := fmt.Errorf("max restarts exceeded: %v", m.config.RestartConfig.MaxRestarts)
			m.config.Logger.Debug(err.Error())
			return err
		}
	}

	err := m.StopPlugin(pm)
	if err != nil {
		m.config.Logger.Error("failed to stop plugin: %v", err)
		return err
	}

	p, err = m.StartPlugin(pm)
	if err != nil {
		m.config.Logger.Error("failed to delete plugin: %v", err)
		return err
	}
	p.RestartCount = restartCount + 1

	m.config.Logger.Debug("restarted plugin: %v", pm)
	return nil
}

func (m *Manager[C]) supervisor() {
	defer close(m.done)

	for {
		select {
		case pm := <-m.killed:
			if m.config.RestartConfig.Managed {
				m.RestartPlugin(pm)
			} else {
				restartFunc := m.config.RestartConfig.RestartFunc
				if restartFunc != nil {
					restartFunc(m.config.Logger, pm)
				}
			}

		case s := <-m.stop:
			log.Printf("tomb is dying?? %v\n", s)
			return
		}
	}

}

func (m *Manager[C]) Close() error {
	for _, p := range m.plugins {
		p.Kill()
	}
	close(m.stop)
	<-m.done
	return nil
}

func (m *Manager[C]) GetPlugin(pluginKey string) (*PluginInstance[C], bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.plugins[pluginKey]
	return p, ok
}

func (m *Manager[C]) insertPlugin(pluginKey string, p *PluginInstance[C]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.plugins[pluginKey] = p
	return nil
}

func (m *Manager[C]) deletePlugin(pluginKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.plugins, pluginKey)
	return nil
}
