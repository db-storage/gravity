package pgsql_stream

import (
	"encoding/binary"
	"fmt"

	"github.com/juju/errors"

	_ "github.com/json-iterator/go"

	_ "github.com/moiot/gravity/pkg/config"
	_ "github.com/moiot/gravity/pkg/pgsql"
)

type StringInfoData struct {
	msg []byte
	//The next byte to read
	cursor int
}

func (s *StringInfoData) HasMoreData() bool {
	return s.cursor < len(s.msg)
}

func (s *StringInfoData) checkRemain(count int) error {
	if s.cursor+count > len(s.msg) {
		return errors.Errorf("[pgsql] not enough data, len: %d, cursor: %d, count: %d", len(s.msg), s.cursor, count)
	}
	return nil
}

func (s *StringInfoData) GetUInt8() (uint8, error) {
	if err := s.checkRemain(1); err != nil {
		return 0, err
	}
	val := uint8(s.msg[s.cursor])
	s.cursor++
	return val, nil
}

func (s *StringInfoData) GetInt8() (int8, error) {
	if err := s.checkRemain(1); err != nil {
		return 0, err
	}
	val := int8(s.msg[s.cursor])
	s.cursor++
	return val, nil
}

func (s *StringInfoData) GetInt64() (int64, error) {
	if err := s.checkRemain(8); err != nil {
		return 0, err
	}
	val := int64(binary.BigEndian.Uint64(s.msg[s.cursor : s.cursor+8]))
	s.cursor += 8
	return val, nil
}

func (s *StringInfoData) GetUInt64() (uint64, error) {
	if err := s.checkRemain(8); err != nil {
		return 0, err
	}
	val := binary.BigEndian.Uint64(s.msg[s.cursor : s.cursor+8])
	s.cursor += 8
	return val, nil
}

func (s *StringInfoData) GetInt32() (int32, error) {
	if err := s.checkRemain(4); err != nil {
		return 0, err
	}
	val := int32(binary.BigEndian.Uint32(s.msg[s.cursor : s.cursor+4]))
	s.cursor += 4
	return val, nil
}

func (s *StringInfoData) GetUInt32() (uint32, error) {
	if err := s.checkRemain(4); err != nil {
		return 0, err
	}
	val := binary.BigEndian.Uint32(s.msg[s.cursor : s.cursor+4])
	s.cursor += 4
	return val, nil
}

func (s *StringInfoData) GetInt16() (int16, error) {
	if err := s.checkRemain(2); err != nil {
		return 0, err
	}
	val := int16(binary.BigEndian.Uint16(s.msg[s.cursor : s.cursor+2]))
	s.cursor += 2
	return val, nil
}

func (s *StringInfoData) GetUInt16() (uint16, error) {
	if err := s.checkRemain(2); err != nil {
		return 0, err
	}
	val := binary.BigEndian.Uint16(s.msg[s.cursor : s.cursor+2])
	s.cursor += 2
	return val, nil
}

func (s *StringInfoData) GetBytes(count int) ([]byte, error) {
	if err := s.checkRemain(count); err != nil {
		return nil, err
	}
	val := s.msg[s.cursor : s.cursor+count]
	s.cursor += count
	return val, nil

}

func (s *StringInfoData) GetString() (string, error) {
	for i := s.cursor; i < len(s.msg); i++ {
		if s.msg[i] == 0 {
			result := s.msg[s.cursor:i]
			s.cursor = i + 1
			return string(result), nil
		}
	}
	return "", fmt.Errorf("No String found, len:%d, cursor:%d", len(s.msg), s.cursor)
}
