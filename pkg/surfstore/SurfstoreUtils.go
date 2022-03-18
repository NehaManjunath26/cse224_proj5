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

	blockSize := client.BlockSize
	baseDir := client.BaseDir

	curr_items, err := ioutil.ReadDir(baseDir)
	if err != nil {
		log.Panic("Error reading directory")
	}

	localMetaMap := make(map[string][]string)

	for _, file := range curr_items {
		name := file.Name()
		if name == DEFAULT_META_FILENAME || file.IsDir() {
			continue
		}

		fileHashList := make([]string, 0)
		nameOpen, _ := filepath.Abs(ConcatPath(baseDir, name))
		fh, err := os.Open(nameOpen)
		if err != nil {
			log.Panicf("error reading file %v: %v", nameOpen, err)
		}

		for {
			fileContent := make([]byte, blockSize)
			readBytes, err := fh.Read(fileContent)
			fileContent = fileContent[:readBytes]
			if err != nil || readBytes == 0 {
				break
			}
			blockhash := GetBlockHashString(fileContent)
			fileHashList = append(fileHashList, blockhash)
		}
		localMetaMap[name] = fileHashList
	}

	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	if _, err := os.Stat(metaFilePath); err != nil {
		// if index.txt is not there, create it
		fh, err := os.Create(metaFilePath)
		if err != nil {
			fmt.Println(err)
		} else {
			fh.Close()
		}
	}

	// scan the index file
	indexMetaMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		fmt.Println(err)
	}

	for fileName, localHashList := range localMetaMap {
		indexMetaData, ok := indexMetaMap[fileName]
		if !ok {
			// case 3:
			// there in currrent directory, not there in index
			// created in current directory, create in index
			newFileMetaData := &FileMetaData{
				Filename:      fileName,
				Version:       1,
				BlockHashList: localHashList,
			}
			indexMetaMap[fileName] = newFileMetaData
		} else {
			// there in current directory, there in index
			// check if hashes are same
			if !isEqual(indexMetaData.BlockHashList, localHashList) {
				// Case 2:
				// file modified in local.
				// update the file in index
				indexMetaMap[fileName].Version += 1
				indexMetaMap[fileName].BlockHashList = localHashList
			} else {
				// Case 1:
				// nothing to do
				continue
			}
		}
	}

	tombStone := make([]string, 0)
	tombStone = append(tombStone, "0")
	for fileName := range indexMetaMap {
		// check if file is not in local.
		_, ok := localMetaMap[fileName]
		if !ok {
			// Case 4:
			// deleted from local
			indexMetaMap[fileName].BlockHashList = tombStone
			indexMetaMap[fileName].Version += 1
		} else {
			// Case 5:
			// already handled above
			continue
		}
	}

	remoteMetaMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteMetaMap); err != nil {
		log.Fatal(err)
	}

	for fileName := range remoteMetaMap {
		indexMetaData, ok := indexMetaMap[fileName]
		if !ok {
			indexMetaMap[fileName] = remoteMetaMap[fileName]
			if !isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
				if err := getBlocksAndWriteToFile(remoteMetaMap[fileName], addr, &client); err != nil {
					log.Panic("error: ", err)
				}
			}
		} else if !isEqual(indexMetaData.BlockHashList, remoteMetaMap[fileName].BlockHashList) {
			if indexMetaData.Version == remoteMetaMap[fileName].Version+1 {
				PutfileName, _ := filepath.Abs(ConcatPath(baseDir, fileName))
				if _, err := os.Stat(PutfileName); err == nil {
					localHashMap, hashesIn, err_get := getHashFromFile(fileName, int32(blockSize), baseDir)
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
					indexMetaMap[fileName] = remoteMetaMap[fileName]

					if !isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
						if err := getBlocksAndWriteToFile(remoteMetaMap[fileName], addr, &client); err != nil {
							log.Panic("error: ", err)
						}
					} else if isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
						indexMetaMap[fileName] = remoteMetaMap[fileName]
						delFile, _ := filepath.Abs(ConcatPath(baseDir, fileName))
						if err := os.Remove(delFile); err != nil {
							log.Panic("error:", err)
						}
					}
				}
			} else if remoteMetaMap[fileName].Version+1 > indexMetaData.Version {
				indexMetaMap[fileName] = remoteMetaMap[fileName]

				if !isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
					if err := getBlocksAndWriteToFile(remoteMetaMap[fileName], addr, &client); err != nil {
						log.Panic("error: ", err)
					}
				} else if isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
					indexMetaMap[fileName] = remoteMetaMap[fileName]
					delFile, _ := filepath.Abs(ConcatPath(baseDir, fileName))
					if err := os.Remove(delFile); err != nil {
						log.Panic("error:", err)
					}
				}
			}
		} else if indexMetaData.Version != remoteMetaMap[fileName].Version {
			indexMetaMap[fileName].Version = remoteMetaMap[fileName].Version
		}

	}

	for fileName := range indexMetaMap {
		if fileName == DEFAULT_META_FILENAME {
			continue
		}
		_, ok := remoteMetaMap[fileName]
		if !ok {
			PutfileName, _ := filepath.Abs(ConcatPath(baseDir, fileName))
			if _, err := os.Stat(PutfileName); err == nil {
				localHashMap, hashesIn, err := getHashFromFile(fileName, int32(blockSize), baseDir)
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
			if err := client.UpdateFile(indexMetaMap[fileName], newVersion); err != nil {
				fmt.Println(err)
			}
			if *newVersion == -1 {
				indexMetaMap[fileName] = remoteMetaMap[fileName]

				if !isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
					if err := getBlocksAndWriteToFile(remoteMetaMap[fileName], addr, &client); err != nil {
						log.Panic("error: ", err)
					}
				} else if isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
					indexMetaMap[fileName] = remoteMetaMap[fileName]
					delFile, _ := filepath.Abs(ConcatPath(baseDir, fileName))
					if err := os.Remove(delFile); err != nil {
						log.Panic("error:", err)
					}
				}
			}
		} else {
			continue
		}
	}

	if err := WriteMetaFile(indexMetaMap, baseDir); err != nil {
		log.Panic("error", err)
	}
}

func getBlocksAndWriteToFile(remoteMetaData *FileMetaData, blockStoreAddr string, client *RPCClient) error {
	filename := remoteMetaData.Filename
	filename, _ = filepath.Abs(ConcatPath(client.BaseDir, filename))

	fh, err_create := os.Create(filename)
	if err_create != nil {
		return err_create
	}
	defer fh.Close()

	hashList := remoteMetaData.BlockHashList

	for _, hashValue := range hashList {
		blockReturn := &Block{}
		if err := client.GetBlock(hashValue, blockStoreAddr, blockReturn); err != nil {
			return err
		}

		_, err := fh.Write(blockReturn.BlockData)
		if err != nil {
			return err
		}
	}

	return nil
}

func isEqual(str1, str2 []string) bool {
	return s.Join(str1, "") == s.Join(str2, "")
}

func getHashFromFile(fileName string, blockSize int32, baseDir string) (map[string]*Block, []string, error) {
	localHashMap := make(map[string]*Block)
	fileName, _ = filepath.Abs(ConcatPath(baseDir, fileName))
	fh, err := os.Open(fileName)
	if err != nil {
		log.Printf("Error reading file %v: %v", fileName, err)
		return nil, nil, err
	}

	localHashList := make([]string, 0)

	for {
		fileContent := make([]byte, blockSize)
		readBytes, err := fh.Read(fileContent)
		fileContent = fileContent[:readBytes]
		if err != nil || readBytes == 0 {
			break
		}
		blockhash := GetBlockHashString(fileContent)
		localHashList = append(localHashList, blockhash)
		localHashMap[blockhash] = &Block{
			BlockData: fileContent,
			BlockSize: int32(readBytes),
		}
	}

	return localHashMap, localHashList, nil
}
