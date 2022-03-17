package surfstore

import (
	context "context"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		err := errors.New("cannot find the block")
		return nil, err
	} else {
		return block, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	data, _ := block.BlockData, block.BlockSize
	hash := GetBlockHashString(data)
	bs.BlockMap[hash] = block

	return &Success{Flag: true}, nil

}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHashesNotPresent := new(BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; !ok {
			blockHashesNotPresent.Hashes = append(blockHashesNotPresent.Hashes, hash)
		}
	}

	return blockHashesNotPresent, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
