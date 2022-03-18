package surfstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	s "strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var addr string
	if err := client.GetBlockStoreAddr(&addr); err != nil {
		log.Fatal(err)
	}
	baseDirectory := client.BaseDir
	lMap := make(map[string][]string)
	blkSz := client.BlockSize

	cur, err := ioutil.ReadDir(baseDirectory)
	if err != nil {
		log.Panic("err : read dir")
	}
	for _, file := range cur {
		name := file.Name()
		if name == DEFAULT_META_FILENAME || file.IsDir() {
			continue
		}

		fhl := make([]string, 0)
		nameOp, _ := filepath.Abs(ConcatPath(baseDirectory, name))
		fh, err := os.Open(nameOp)
		if err != nil {
			log.Panicf("err : read file %v: %v", nameOp, err)
		}

		for {
			content := make([]byte, blkSz)
			rb, err := fh.Read(content)
			content = content[:rb]
			if err != nil || rb == 0 {
				break
			}
			blkHash := GetBlockHashString(content)
			fhl = append(fhl, blkHash)
		}
		lMap[name] = fhl
	}

	metaPath, _ := filepath.Abs(ConcatPath(baseDirectory, DEFAULT_META_FILENAME))
	if _, err := os.Stat(metaPath); err != nil {
		fh, err := os.Create(metaPath)
		if err != nil {
			fmt.Println(err)
		} else {
			fh.Close()
		}
	}

	idxMap, err := LoadMetaFromMetaFile(baseDirectory)
	if err != nil {
		fmt.Println(err)
	}
	for fName, localList := range lMap {
		idxdata, ok := idxMap[fName]
		if !ok {
			newFileMetaData := &FileMetaData{
				Filename:      fName,
				Version:       1,
				BlockHashList: localList,
			}
			idxMap[fName] = newFileMetaData
		} else {
			if !isEqual(idxdata.BlockHashList, localList) {
				idxMap[fName].Version += 1
				idxMap[fName].BlockHashList = localList
			} else {
				continue
			}
		}
	}
	ts := make([]string, 0)
	ts = append(ts, "0")
	for fName := range idxMap {
		_, ok := lMap[fName]
		if !ok {
			idxMap[fName].BlockHashList = ts
			idxMap[fName].Version += 1
		} else {
			continue
		}
	}

	rMetaMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&rMetaMap); err != nil {
		log.Fatal(err)
	}

	for fileName := range rMetaMap {
		idxMData, ok := idxMap[fileName]
		if !ok {
			idxMap[fileName] = rMetaMap[fileName]
			if !isEqual(rMetaMap[fileName].BlockHashList, ts) {
				if err := writeBlocks(rMetaMap[fileName], addr, &client); err != nil {
					log.Panic("error: ", err)
				}
			}
		} else if !isEqual(idxMData.BlockHashList, rMetaMap[fileName].BlockHashList) {
			if idxMData.Version == rMetaMap[fileName].Version+1 {
				pFile, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
				if _, err := os.Stat(pFile); err == nil {
					localHashMap, hashesIn, err_get := getHash(fileName, int32(blkSz), baseDirectory)
					if err_get != nil {
						log.Panic("error", err)
					}
					hashesOut := make([]string, 0)
					if err := client.HasBlocks(hashesIn, addr, &hashesOut); err != nil {
						log.Panic("error", err)
					}

					succ := new(bool)
					for _, notHash := range hashesOut {
						if err := client.PutBlock(localHashMap[notHash], addr, succ); err != nil {
							log.Panic("error", err)
						}
					}
				}

				newVersion := new(int32)
				if err := client.UpdateFile(idxMData, newVersion); err != nil {
					fmt.Println(err)
				}

				if *newVersion == -1 {
					idxMap[fileName] = rMetaMap[fileName]
					if !isEqual(rMetaMap[fileName].BlockHashList, ts) {
						if err := writeBlocks(rMetaMap[fileName], addr, &client); err != nil {
							log.Panic("error: ", err)
						}
					} else if isEqual(rMetaMap[fileName].BlockHashList, ts) {
						idxMap[fileName] = rMetaMap[fileName]
						delFile, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
						if err := os.Remove(delFile); err != nil {
							log.Panic("error:", err)
						}
					}
				}
			} else if rMetaMap[fileName].Version+1 > idxMData.Version {
				idxMap[fileName] = rMetaMap[fileName]
				if !isEqual(rMetaMap[fileName].BlockHashList, ts) {
					if err := writeBlocks(rMetaMap[fileName], addr, &client); err != nil {
						log.Panic("error: ", err)
					}
				} else if isEqual(rMetaMap[fileName].BlockHashList, ts) {
					idxMap[fileName] = rMetaMap[fileName]
					delFile, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
					if err := os.Remove(delFile); err != nil {
						log.Panic("error:", err)
					}
				}
			}
		} else if idxMData.Version != rMetaMap[fileName].Version {
			idxMap[fileName].Version = rMetaMap[fileName].Version
		}

	}

	for fileName := range idxMap {
		if fileName == DEFAULT_META_FILENAME {
			continue
		}
		_, ok := rMetaMap[fileName]
		if !ok {
			pFileName, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
			if _, err := os.Stat(pFileName); err == nil {
				localHashMap, hashesIn, err := getHash(fileName, int32(blkSz), baseDirectory)
				if err != nil {
					log.Panic("error", err)
				}
				hashesPut := make([]string, 0)
				if err := client.HasBlocks(hashesIn, addr, &hashesPut); err != nil {
					log.Panic("error", err)
				}

				succ := new(bool)
				for _, notHash := range hashesPut {
					if err := client.PutBlock(localHashMap[notHash], addr, succ); err != nil {
						log.Panic("error", err)
					}
				}
			}
			newVersion := new(int32)
			if err := client.UpdateFile(idxMap[fileName], newVersion); err != nil {
				fmt.Println(err)
			}
			if *newVersion == -1 {
				idxMap[fileName] = rMetaMap[fileName]

				if !isEqual(rMetaMap[fileName].BlockHashList, ts) {
					if err := writeBlocks(rMetaMap[fileName], addr, &client); err != nil {
						log.Panic("error: ", err)
					}
				} else if isEqual(rMetaMap[fileName].BlockHashList, ts) {
					idxMap[fileName] = rMetaMap[fileName]
					delFile, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
					if err := os.Remove(delFile); err != nil {
						log.Panic("error:", err)
					}
				}
			}
		} else {
			continue
		}
	}

	if err := WriteMetaFile(idxMap, baseDirectory); err != nil {
		log.Panic("error", err)
	}
}

func writeBlocks(remoteMetaData *FileMetaData, blkAddr string, client *RPCClient) error {
	name := remoteMetaData.Filename
	name, _ = filepath.Abs(ConcatPath(client.BaseDir, name))

	fh, err_create := os.Create(name)
	if err_create != nil {
		return err_create
	}
	defer fh.Close()

	hl := remoteMetaData.BlockHashList

	for _, val := range hl {
		blkRet := &Block{}
		if err := client.GetBlock(val, blkAddr, blkRet); err != nil {
			return err
		}

		_, err := fh.Write(blkRet.BlockData)
		if err != nil {
			return err
		}
	}

	return nil
}

func isEqual(str1, str2 []string) bool {
	return s.Join(str1, "") == s.Join(str2, "")
}

func getHash(name string, blockSize int32, baseDir string) (map[string]*Block, []string, error) {
	lhm := make(map[string]*Block)
	name, _ = filepath.Abs(ConcatPath(baseDir, name))
	fh, err := os.Open(name)
	if err != nil {
		log.Printf("Error reading file %v: %v", name, err)
		return nil, nil, err
	}

	localList := make([]string, 0)

	for {
		content := make([]byte, blockSize)
		bytes, err := fh.Read(content)
		content = content[:bytes]
		if err != nil || bytes == 0 {
			break
		}
		blkHash := GetBlockHashString(content)
		localList = append(localList, blkHash)
		lhm[blkHash] = &Block{
			BlockData: content,
			BlockSize: int32(bytes),
		}
	}
	return lhm, localList, nil
}
