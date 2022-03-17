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

	// check version number
	incomingVersion := fileMetaData.Version

	remoteFile, ok := m.FileMetaMap[fileMetaData.Filename]
	if ok {
		currVersion := remoteFile.Version
		if incomingVersion != currVersion+1 {
			err := errors.New("file version mismatch")
			return &Version{Version: -1}, err
		}
	} else {
		if incomingVersion != 1 {
			return &Version{Version: -1}, fmt.Errorf("version num has to be 1")
		}
	}

	// update the blockHashList
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData

	// return new version
	return &Version{Version: incomingVersion}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
