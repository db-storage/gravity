package position_store

import (
	_ "github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/utils"
	log "github.com/sirupsen/logrus"
)

//PipelineGravityMySQLPosition
type PipelineGravityPgSQLPosition struct {
	CurrentPosition *utils.PgSQLPosition `json:"current_position"`
	StartPosition   *utils.PgSQLPosition `json:"start_position"`
}

func (p *PipelineGravityPgSQLPosition) String() string {
	return p.GetRaw()
}

func (p *PipelineGravityPgSQLPosition) Get() interface{} {
	ret := PipelineGravityPgSQLPosition{}
	if p.CurrentPosition != nil {
		pos := *p.CurrentPosition
		ret.CurrentPosition = &pos
	}
	if p.StartPosition != nil {
		pos := *p.StartPosition
		ret.StartPosition = &pos
	}
	return ret
}

func (p *PipelineGravityPgSQLPosition) GetRaw() string {
	s, _ := myJson.MarshalToString(p)
	return s
}

func (p *PipelineGravityPgSQLPosition) Put(pos interface{}) {
	*p = pos.(PipelineGravityPgSQLPosition)
}

func (p *PipelineGravityPgSQLPosition) PutRaw(pos string) {
	if err := myJson.UnmarshalFromString(pos, p); err != nil {
		log.Fatalf("[PipelineGravityPgSQLPosition.PutRaw] %s", err)
	}
}

func (p *PipelineGravityPgSQLPosition) Stage() config.InputMode {
	return config.Stream
}
