package surfstore

import (
	context "context"
	"errors"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	inputVer := fileMetaData.Version

	file, ok := m.FileMetaMap[fileMetaData.Filename]
	if ok {
		newVer := file.Version
		if inputVer != newVer+1 {
			err := errors.New("version mismatch for file")
			return &Version{Version: -1}, err
		}
	} else {
		if inputVer != 1 {
			return &Version{Version: -1}, fmt.Errorf("version num not 1")
		}
	}
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	return &Version{Version: inputVer}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
