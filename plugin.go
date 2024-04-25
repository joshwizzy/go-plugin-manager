package manager

import (
	"log"
	"time"

	"github.com/hashicorp/go-hclog"
	goplugin "github.com/hashicorp/go-plugin"
)

type PluginInfo struct {
	BinPath  string
	Key      string
	Checksum string
	Restarts int
}

type pluginInstance[T any] struct {
	Impl      T
	client    *goplugin.Client
	rpcClient goplugin.ClientProtocol
	Info      PluginInfo
	stop      chan struct{}
	done      chan struct{}
}

func (p *pluginInstance[T]) Kill() {
	p.client.Kill()
}

func (p *pluginInstance[T]) Ping() error {
	return p.rpcClient.Ping()
}

func (p *pluginInstance[T]) Watch(
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
		}
	}
}

func (p *pluginInstance[T]) Stop() {
	close(p.stop)
	<-p.done
	p.client.Kill()
}
