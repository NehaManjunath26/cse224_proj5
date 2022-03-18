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
	b, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		err := errors.New("block not found")
		return nil, err
	}
	return b, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	h := GetBlockHashString(block.BlockData)
	bs.BlockMap[h] = block
	return &Success{Flag: true}, nil
}

func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHashesAbsent := new(BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; !ok {
			blockHashesAbsent.Hashes = append(blockHashesAbsent.Hashes, hash)
		}
	}

	return blockHashesAbsent, nil
}

var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
