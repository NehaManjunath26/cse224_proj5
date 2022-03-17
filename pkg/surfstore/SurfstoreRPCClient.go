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
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}

	*succ = success.Flag

	if !(*succ) {
		return fmt.Errorf("cannot put block: %s", "err")
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
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	blockHashesIn_obj := &BlockHashes{Hashes: blockHashesIn}
	bout, err := c.HasBlocks(ctx, blockHashesIn_obj)

	if err != nil {
		conn.Close()
		return err
	}

	*blockHashesOut = bout.Hashes

	return conn.Close()

}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {

	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// return err
			continue
		}

		defer conn.Close()
		m := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		empty := new(emptypb.Empty)
		FileInfoMap, err := m.GetFileInfoMap(ctx, empty)

		if err != nil {
			conn.Close()
			continue
		}

		*serverFileInfoMap = FileInfoMap.FileInfoMap

		return conn.Close()

	}

	return errors.New("servers not found")

}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {

	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		m := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		updatedVersion, err := m.UpdateFile(ctx, fileMetaData)

		if err != nil {
			conn.Close()
			continue
		}

		*latestVersion = updatedVersion.Version

		return conn.Close()
	}

	return errors.New("all servers down")

}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {

	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		defer conn.Close()
		m := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		empty := new(emptypb.Empty)
		addr, err := m.GetBlockStoreAddr(ctx, empty)

		if err != nil {
			conn.Close()
			return err
		}

		*blockStoreAddr = addr.Addr

		return conn.Close()
	}

	return errors.New("all servers down")

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
