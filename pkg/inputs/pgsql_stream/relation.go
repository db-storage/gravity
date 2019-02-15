package pgsql_stream

import (
	//"context"
	//"encoding/binary"
	//"fmt"
	//"time"

	"github.com/jackc/pgx"
	"github.com/juju/errors"
	//"github.com/moiot/gravity/pkg/utils"

	_ "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

//These structures are used to handle pglogical's "Relation" message.
//It is different from Schema, only column id and name is included,
// no type or primary key, default value...

type Attr struct {
	Idx  int    `json:"idx"`
	Name string `json:"name"`
}

// Table
type Relation struct {
	//schema string `json:"db_name"`
	name string `json:"table_name"`

	attrs   []Attr `json:"attrs"`
	attrMap map[int]*Attr
	//once    sync.Once
}

func (rel *Relation) AddAttr(att Attr) {
	rel.attrs = append(rel.attrs, att)
}

func (rel *Relation) Clear() {
	rel.attrs = nil
	rel.attrMap = nil
}

func (rel *Relation) buildMap() {
	rel.attrMap = make(map[int]*Attr, len(rel.attrs))
	for _, attr := range rel.attrs {
		rel.attrMap[attr.Idx] = &attr
	}
}

func (rel *Relation) tryBuildMap() {
	if rel.attrs != nil && rel.attrMap == nil {
		rel.buildMap()
		return
	}
}

func (rel *Relation) AttrNameFromId(id int) (string, error) {
	rel.tryBuildMap()

	if rel.attrMap == nil {
		return "", errors.Errorf("attrMap is empty")
	}

	attr, ok := rel.attrMap[id]
	if ok {
		return attr.Name, nil
	}
	return "", errors.Errorf("[pgsql] no attr found for: %v, attrs: %v", id, rel.attrs)

}

type Relations map[string]*Relation

func (rm *Relations) UpdateRelation(rel *Relation) {
	(*rm)[rel.name] = rel
}

func ReadAttrs(walData *StringInfoData, rel *Relation) error {
	blockType, err := walData.GetUInt8()
	if err != nil {
		return err
	}
	if blockType != 'A' {
		return errors.Errorf("[pgsql] unkown blockType: %v", blockType)
	}

	numAttrs, err := walData.GetInt32()
	if err != nil {
		return err
	}
	var i int32
	for i = 1; i < numAttrs; i++ {
		blockType, err = walData.GetUInt8()
		if err != nil {
			return err
		}

		if blockType != 'C' {
			return errors.Errorf("[pgsql] Unkown blockType: %v, expected: C", blockType)
		}

		_, err := walData.GetUInt8()
		if err != nil {
			//flag ignored
			return err
		}

		blockType, err = walData.GetUInt8()
		if err != nil {
			return err
		}

		if blockType != 'N' {
			return errors.Errorf("[pgsql] Unkown blockType: %v, expected: C", blockType)
		}

		nameLen, err := walData.GetInt32()
		if err != nil {
			return err
		}

		name, err := walData.GetBytes((int)(nameLen))
		if err != nil {
			return err
		}

		attr := Attr{Idx: (int)(i), Name: string(name)}
		rel.AddAttr(attr)
		log.Infof("[pgsql] get rel: %v", rel)
	}
	return nil
}

func RelationFromMsg(repMsg *pgx.ReplicationMessage) (*Relation, error) {
	walData := StringInfoData{repMsg.WalMessage.WalData, 1}

	var flags uint8
	if _, err := walData.GetUInt8(); err != nil {
		return nil, err
	}

	if flags != 0 {
		return nil, errors.Errorf("[pgsql] Unkown flags: %v", flags)
	}

	var rel Relation

	nameLen, err := walData.GetInt32()
	if err != nil {
		return nil, err
	}

	name, err := walData.GetBytes((int)(nameLen))
	if err != nil {
		return nil, err
	}

	rel.name = string(name)

	if err = ReadAttrs(&walData, &rel); err != nil {
		return nil, err
	}
	return &rel, nil

}
