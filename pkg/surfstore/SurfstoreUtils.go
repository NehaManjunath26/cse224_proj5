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
	baseDirectory := client.BaseDir
	blkSz := client.BlockSize
	localMap := make(map[string][]string)
	var addr string
	if err := client.GetBlockStoreAddr(&addr); err != nil {
		log.Fatal(err)
	}

	cur, err := ioutil.ReadDir(baseDirectory)
	if err != nil {
		log.Panic("Reading directory failed")
	}

	for _, f := range cur {
		n := f.Name()
		if n == DEFAULT_META_FILENAME || f.IsDir() {
			continue
		}

		hashList := make([]string, 0)
		name, _ := filepath.Abs(ConcatPath(baseDirectory, n))
		fh, err := os.Open(name)
		if err != nil {
			log.Panicf("err in file read %v: %v", name, err)
		}

		for {
			content := make([]byte, blkSz)
			rBytes, err := fh.Read(content)
			content = content[:rBytes]
			if err != nil || rBytes == 0 {
				break
			}
			hashList = append(hashList, GetBlockHashString(content))
		}
		localMap[n] = hashList
	}

	metaFile, _ := filepath.Abs(ConcatPath(baseDirectory, DEFAULT_META_FILENAME))
	if _, err := os.Stat(metaFile); err != nil {
		fh, err := os.Create(metaFile)
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

	for fileName, localHashList := range localMap {
		indexMetaData, ok := idxMap[fileName]
		if !ok {
			newFileMetaData := &FileMetaData{
				BlockHashList: localHashList,
				Filename:      fileName,
				Version:       1,
			}
			idxMap[fileName] = newFileMetaData
		} else {
			if !isEqual(indexMetaData.BlockHashList, localHashList) {
				idxMap[fileName].Version += 1
				idxMap[fileName].BlockHashList = localHashList
			} else {
				continue
			}
		}
	}

	ts := make([]string, 0)
	ts = append(ts, "0")
	for fileName := range idxMap {
		_, ok := localMap[fileName]
		if !ok {
			idxMap[fileName].BlockHashList = ts
			idxMap[fileName].Version += 1
		} else {
			continue
		}
	}

	remoteMetaMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteMetaMap); err != nil {
		log.Fatal(err)
	}

	for fileName := range remoteMetaMap {
		indexMetaData, ok := idxMap[fileName]
		if !ok {
			idxMap[fileName] = remoteMetaMap[fileName]
			if !isEqual(remoteMetaMap[fileName].BlockHashList, ts) {
				if err := writeBlocks(remoteMetaMap[fileName], addr, &client); err != nil {
					log.Panic("error: ", err)
				}
			}
		} else if !isEqual(indexMetaData.BlockHashList, remoteMetaMap[fileName].BlockHashList) {
			if indexMetaData.Version == remoteMetaMap[fileName].Version+1 {
				PutfileName, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
				if _, err := os.Stat(PutfileName); err == nil {
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
				if err := client.UpdateFile(indexMetaData, newVersion); err != nil {
					fmt.Println(err)
				}

				if *newVersion == -1 {
					idxMap[fileName] = remoteMetaMap[fileName]

					if !isEqual(remoteMetaMap[fileName].BlockHashList, ts) {
						if err := writeBlocks(remoteMetaMap[fileName], addr, &client); err != nil {
							log.Panic("error: ", err)
						}
					} else if isEqual(remoteMetaMap[fileName].BlockHashList, ts) {
						idxMap[fileName] = remoteMetaMap[fileName]
						delFile, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
						if err := os.Remove(delFile); err != nil {
							log.Panic("error:", err)
						}
					}
				}
			} else if remoteMetaMap[fileName].Version+1 > indexMetaData.Version {
				idxMap[fileName] = remoteMetaMap[fileName]

				if !isEqual(remoteMetaMap[fileName].BlockHashList, ts) {
					if err := writeBlocks(remoteMetaMap[fileName], addr, &client); err != nil {
						log.Panic("error: ", err)
					}
				} else if isEqual(remoteMetaMap[fileName].BlockHashList, ts) {
					idxMap[fileName] = remoteMetaMap[fileName]
					delFile, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
					if err := os.Remove(delFile); err != nil {
						log.Panic("error:", err)
					}
				}
			}
		} else if indexMetaData.Version != remoteMetaMap[fileName].Version {
			idxMap[fileName].Version = remoteMetaMap[fileName].Version
		}

	}

	for fileName := range idxMap {
		if fileName == DEFAULT_META_FILENAME {
			continue
		}
		_, ok := remoteMetaMap[fileName]
		if !ok {
			PutfileName, _ := filepath.Abs(ConcatPath(baseDirectory, fileName))
			if _, err := os.Stat(PutfileName); err == nil {
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
				idxMap[fileName] = remoteMetaMap[fileName]

				if !isEqual(remoteMetaMap[fileName].BlockHashList, ts) {
					if err := writeBlocks(remoteMetaMap[fileName], addr, &client); err != nil {
						log.Panic("error: ", err)
					}
				} else if isEqual(remoteMetaMap[fileName].BlockHashList, ts) {
					idxMap[fileName] = remoteMetaMap[fileName]
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

func writeBlocks(remoteMetaData *FileMetaData, blockStoreAddr string, client *RPCClient) error {
	name := remoteMetaData.Filename
	name, _ = filepath.Abs(ConcatPath(client.BaseDir, name))

	fh, err := os.Create(name)
	if err != nil {
		return err
	}
	defer fh.Close()

	hl := remoteMetaData.BlockHashList

	for _, val := range hl {
		blkRet := &Block{}
		if err := client.GetBlock(val, blockStoreAddr, blkRet); err != nil {
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

func getHash(fileName string, blkSz int32, baseDirectory string) (map[string]*Block, []string, error) {
	fileName, _ = filepath.Abs(ConcatPath(baseDirectory, fileName))
	fh, err := os.Open(fileName)
	if err != nil {
		log.Printf("Error in file read %v: %v", fileName, err)
		return nil, nil, err
	}
	hl := make([]string, 0)
	hm := make(map[string]*Block)
	for {
		content := make([]byte, blkSz)
		rb, err := fh.Read(content)
		content = content[:rb]
		if rb == 0 || err != nil {
			break
		}
		blkHash := GetBlockHashString(content)
		hl = append(hl, blkHash)
		hm[blkHash] = &Block{
			BlockData: content,
			BlockSize: int32(rb),
		}
	}

	return hm, hl, nil
}
