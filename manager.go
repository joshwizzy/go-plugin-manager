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
	done         chan struct{}
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
	dying   chan<- PluginInfo
	dead    <-chan PluginInfo
	config  *ManagerConfig
	plugins map[string]*PluginInstance[C]
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

	dying, dead := make(chan PluginInfo), make(chan PluginInfo)
	m := &Manager[C]{
		Name:    pluginKey(config.PluginMap),
		config:  config,
		plugins: make(map[string]*PluginInstance[C]),
		dying:   dying,
		dead:    dead,
	}

	m.t.Go(m.startPluginSupervisor(dying, dead))

	return m
}

func (m *Manager[C]) Dead() <-chan PluginInfo {
	return m.dead
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

	done := make(chan struct{})
	watcher := m.pluginWatcher(m.config.RestartConfig.PingInterval, rpcClient, pm, done)
	m.t.Go(watcher)
	p := &PluginInstance[C]{
		Impl:         impl,
		Kill:         client.Kill,
		RestartCount: 0,
		done:         done,
	}
	return p, nil
}

func (m *Manager[C]) pluginWatcher(
	interval time.Duration,
	rpcClient goplugin.ClientProtocol,
	pm PluginInfo,
	done chan struct{},
) func() error {
	ticker := time.NewTicker(interval)
	f := func() error {
		for {
			select {
			case <-done:
				log.Println("we done")
				return nil
			case <-ticker.C:
				if err := rpcClient.Ping(); err != nil {
					m.config.Logger.Debug("plugin %s exited will restart\n", pm.PluginKey)
					// if p, ok := m.GetPlugin(pm.PluginKey); ok && p.Unloaded() {
					// 	return nil
					// }

					// Non-blocking send or discard
					select {
					case m.dying <- pm:
						// message sent
					default:
						// message dropped
					}
					return nil
				}
			case <-m.t.Dying():
				return nil
			}
		}
	}
	return f
}

func (m *Manager[C]) LoadPlugins(plugins []PluginInfo) error {
	m.Lock()
	defer m.Unlock()

	for _, pm := range plugins {
		p, err := m.loadPlugin(pm)
		if err != nil {
			return err
		}
		m.plugins[pm.PluginKey] = p
	}

	return nil
}

type PluginMetadata struct {
	Key      string `json:"key"`
	Restarts int    `json:"restarts"`
}

func (m *Manager[C]) ListPlugins() ([]PluginMetadata, error) {
	m.Lock()
	defer m.Unlock()

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
	m.config.Logger.Debug("unloading plugin %s\n", pm.PluginKey)

	p, ok := m.GetPlugin(pm.PluginKey)
	if ok {
		if p.Kill != nil {
			p.Kill()
		}
		close(p.done)
	}

	err := m.deletePlugin(pm.PluginKey)
	if err != nil {
		m.config.Logger.Error("failed to delete plugin: %v", err)
		return err
	}

	return nil

}

func (m *Manager[C]) StartPlugin(pm PluginInfo) (*PluginInstance[C], error) {
	log.Printf("loading %v %v...", pm.PluginKey, pm.BinPath)
	p, err := m.loadPlugin(pm)
	if err != nil {
		m.config.Logger.Error("failed to load plugin: %v", err)
		return nil, err
	}

	err = m.insertPlugin(pm.PluginKey, p)
	if err != nil {
		m.config.Logger.Error("failed to insert", err)
		return nil, err
	}
	return p, nil

}

func (m *Manager[C]) RestartPlugin(pm PluginInfo) {
	m.config.Logger.Debug("restarting plugin %s\n", pm.PluginKey)
	restartCount := 0
	p, ok := m.GetPlugin(pm.PluginKey)
	if ok {
		restartCount = p.RestartCount
		if restartCount >= m.config.RestartConfig.MaxRestarts {
			m.config.Logger.Debug("max restarts exceeded: %v\n", m.config.RestartConfig.MaxRestarts)
			return
		}
	}

	err := m.StopPlugin(pm)
	if err != nil {
		m.config.Logger.Error("failed to stop plugin: %v", err)
		return
	}

	p, err = m.StartPlugin(pm)
	if err != nil {
		m.config.Logger.Error("failed to delete plugin: %v", err)
		return
	}
	p.RestartCount = restartCount + 1

	m.config.Logger.Debug("restarted plugin: %v", pm)
}

func (m *Manager[C]) startPluginSupervisor(dying <-chan PluginInfo, dead chan<- PluginInfo) func() error {
	return func() error {
		for {
			select {
			case pm := <-dying:
				if m.config.RestartConfig.Managed {
					m.RestartPlugin(pm)
				} else {
					// dead <- pm
					select {
					case dead <- pm:
						// message sent
					default:
						// message dropped
					}
				}

			case s := <-m.t.Dying():
				log.Printf("tomb is dying?? %v\n", s)
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

func (m *Manager[C]) GetPlugin(pluginKey string) (*PluginInstance[C], bool) {
	m.Lock()
	defer m.Unlock()
	p, ok := m.plugins[pluginKey]
	return p, ok
}

func (m *Manager[C]) insertPlugin(pluginKey string, p *PluginInstance[C]) error {
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
