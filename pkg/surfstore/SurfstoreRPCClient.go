package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	blkClient := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := blkClient.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	blkClient := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	op, err := blkClient.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = op.Flag
	if !(*succ) {
		return fmt.Errorf("error in put block: %s", "err")
	} else {
		return conn.Close()
	}
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	blkClient := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hashes := &BlockHashes{Hashes: blockHashesIn}
	bout, err := blkClient.HasBlocks(ctx, hashes)
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = bout.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, adr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(adr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		surfStrClient := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e := new(emptypb.Empty)
		fileInfMap, err := surfStrClient.GetFileInfoMap(ctx, e)
		if err != nil {
			conn.Close()
			continue
		}
		*serverFileInfoMap = fileInfMap.FileInfoMap
		return conn.Close()
	}
	return errors.New("none of the servers are available")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, adr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(adr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		surfStrClient := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		latest, err := surfStrClient.UpdateFile(ctx, fileMetaData)
		if err != nil {
			conn.Close()
			continue
		}
		*latestVersion = latest.Version
		return conn.Close()
	}
	return errors.New("none of the servers are available")

}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	for _, adr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(adr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		surfStrClient := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		empty := new(emptypb.Empty)
		blockStrAdr, err := surfStrClient.GetBlockStoreAddr(ctx, empty)
		if err != nil {
			conn.Close()
			continue
		}
		*blockStoreAddr = blockStrAdr.Addr
		return conn.Close()
	}
	return errors.New("none of the servers are available")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
