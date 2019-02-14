package position_store

import (
	"time"

	_ "github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	_ "github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/utils"
	//log "github.com/sirupsen/logrus"
)

type pgsqlPositionStore struct {
	*dbPositionStore
	startPositionInSpec utils.PgSQLPosition
}

func (s pgsqlPositionStore) Start() error {
	//TODO: support later
	return nil
}

func (s *pgsqlPositionStore) Get() utils.PgSQLPosition {
	posPtr := s.dbPositionStore.Position().Raw.(PipelineGravityPgSQLPosition).CurrentPosition
	if posPtr != nil {
		return *posPtr
	} else {
		return utils.PgSQLPosition{}
	}
}

func (s *pgsqlPositionStore) Put(position utils.PgSQLPosition) {
	prev := s.dbPositionStore.Position().Raw.(PipelineGravityPgSQLPosition)
	if prev.CurrentPosition == nil {
		prev.CurrentPosition = &utils.PgSQLPosition{}
	}
	prev.CurrentPosition = &position

	s.dbPositionStore.Update(Position{
		Name:       s.dbPositionStore.name,
		Stage:      config.Stream,
		Raw:        prev,
		UpdateTime: time.Now(),
	})
}

func (s *pgsqlPositionStore) FSync() {
	//TODO: support later
	//s.dbPositionStore.savePosition()
}

func NewPgsqlPositionStore(pipelineName string, dbConfig *utils.DBConfig, startPosition uint64) (*pgsqlPositionStore, error) {
	store := &dbPositionStore{
		name:       pipelineName,
		db:         nil,
		annotation: "",
		closeC:     make(chan struct{}),
		position:   &PipelineGravityPgSQLPosition{},
		//stage:      delegate.Stage(),
	}

	pgStore := pgsqlPositionStore{dbPositionStore: store, startPositionInSpec: utils.PgSQLPosition{WalPosition: startPosition}}
	return &pgStore, nil
}
