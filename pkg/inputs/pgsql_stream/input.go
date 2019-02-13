package pgsql_stream

import (
	"context"
	"sync"

	"github.com/jackc/pgx"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/pgsql"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"
)

type PluginConfig struct {
	Source        *utils.DBConfig `mapstructure:"source" toml:"source" json:"source"`
	StartPosition uint64          `mapstructure:"start-position" toml:"start-position" json:"start-position"`
}

type pgsqlStreamInputPlugin struct {
	pipelineName string

	cfg *PluginConfig

	emitter core.Emitter
	wg      sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	pgRepSession    *pgx.ReplicationConn
	pglogicalTailer *PglogicalTailer
	//oplogChecker    *OplogChecker
	positionStore position_store.PositionStore

	closeOnce sync.Once
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "pgsql_stream", &pgsqlStreamInputPlugin{}, false)
	log.Info("pgsql_stream plugin registered")

}

// TODO position store, gtm config, etc
func (plugin *pgsqlStreamInputPlugin) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	cfg := PluginConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if cfg.Source == nil {
		return errors.Errorf("no pgsql source confgiured")
	}
	plugin.cfg = &cfg
	return nil
}

func (plugin *pgsqlStreamInputPlugin) NewPositionStore() (position_store.PositionStore, error) {
	//TODO: implement this later
	/*
		positionStore, err := position_store.NewPgsqlPositionStore(plugin.pipelineName, plugin.cfg.Source, plugin.cfg.StartPosition)
		if err != nil {
			return nil, errors.Trace(err)
		}
		plugin.positionStore = positionStore
		return positionStore, nil
	*/
	return nil, nil
}

func (plugin *pgsqlStreamInputPlugin) Start(emitter core.Emitter) error {
	plugin.emitter = emitter
	plugin.ctx, plugin.cancel = context.WithCancel(context.Background())

	session, err := pgsql.NewRepConnection(plugin.cfg.Source)
	if err != nil {
		log.Errorf("Create pgsql rep connection ok")
		return errors.Trace(err)
	}
	log.Infof("Create pgsql rep connection ok")
	plugin.pgRepSession = session

	cfg := plugin.cfg

	tailerOpts := PglogicalTailerOpt{
		session:       session,
		emitter:       emitter,
		ctx:           plugin.ctx,
		sourceHost:    cfg.Source.Host,
		positionStore: plugin.positionStore.(position_store.PgsqlPositionStore),
		pipelineName:  plugin.pipelineName,
	}
	tailer := NewpglogicalTailer(&tailerOpts)

	plugin.pglogicalTailer = tailer
	//plugin.oplogChecker = checker

	plugin.wg.Add(1)
	go func(t *PglogicalTailer) {
		defer plugin.wg.Done()
		t.Run()
	}(tailer)

	plugin.wg.Add(1)

	return nil
}

func (plugin *pgsqlStreamInputPlugin) Stage() config.InputMode {
	return config.Stream
}

func (plugin *pgsqlStreamInputPlugin) PositionStore() position_store.PositionStore {
	return plugin.positionStore
}

func (plugin *pgsqlStreamInputPlugin) Done() chan position_store.Position {
	c := make(chan position_store.Position)
	go func() {
		plugin.Wait()
		c <- plugin.positionStore.Position()
		close(c)
	}()
	return c
}

func (plugin *pgsqlStreamInputPlugin) Wait() {
	plugin.pglogicalTailer.Wait()
}

func (plugin *pgsqlStreamInputPlugin) SendDeadSignal() error {
	return plugin.pgRepSession.Close()
}

func (plugin *pgsqlStreamInputPlugin) Identity() uint32 {
	return 0
}

func (plugin *pgsqlStreamInputPlugin) Close() {
	plugin.closeOnce.Do(func() {
		plugin.cancel()

		log.Infof("[pgsqlStreamInputPlugin] wait others")
		plugin.wg.Wait()
		plugin.pgRepSession.Close()
	})
}
