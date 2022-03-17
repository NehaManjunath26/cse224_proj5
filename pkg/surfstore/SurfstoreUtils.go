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

	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	blockSize := client.BlockSize
	baseDir := client.BaseDir
	// metaAddr := client.MetaStoreAddr

	curr_items, err := ioutil.ReadDir(baseDir)
	if err != nil {
		log.Panic("Error reading directory")
	}

	localMetaMap := make(map[string][]string) // mapping from files in LFD to hashmaps

	// scan local items
	for _, file := range curr_items {
		// ignore directory and special file.
		name := file.Name()
		if name == DEFAULT_META_FILENAME || file.IsDir() {
			continue
		}

		// read file into chunks of blockSize blocks
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

	// sync indexMap to metaFilefoMap
	// put block which aren't there in blockStore <-> updateFileInfo
	// getBlocks which aren't there in local <-> update indexMetaMap
	// handle conflicts

	// get remote file info map.
	remoteMetaMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteMetaMap); err != nil {
		log.Fatal(err)
	}

	for fileName := range remoteMetaMap {
		indexMetaData, ok := indexMetaMap[fileName]
		if !ok {
			// file there in server, not in index.
			// update indexMap with this new entry <-> and also get blocks in local.
			indexMetaMap[fileName] = remoteMetaMap[fileName]
			// TODO: get blocks from remote server to filename
			if !isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
				// Case 1:
				// only write if remote file is not deleted.
				if err := getBlocksAndWriteToFile(remoteMetaMap[fileName], blockStoreAddr, &client); err != nil {
					log.Panic("error: ", err)
				}
			}
		} else if !isEqual(indexMetaData.BlockHashList, remoteMetaMap[fileName].BlockHashList) {
			// file modified in either local or remote.
			if indexMetaData.Version == remoteMetaMap[fileName].Version+1 {
				// if current version is equal to remote version+1, then push the changes <-> also put blocks in remote

				PutfileName, _ := filepath.Abs(ConcatPath(baseDir, fileName))
				if _, err := os.Stat(PutfileName); err == nil {
					// Case 4:
					// remote update is successful.
					// put blocks from fileName to remote server if the update is not delete
					localHashMap, hashesIn, err_get := getHashFromFile(fileName, int32(blockSize), baseDir)
					if err_get != nil {
						log.Panic("error", err)
					}
					hashesOut := make([]string, 0)
					if err := client.HasBlocks(hashesIn, blockStoreAddr, &hashesOut); err != nil {
						log.Panic("error", err)
					}

					succ := new(bool)
					for _, notHash := range hashesOut {
						if err := client.PutBlock(localHashMap[notHash], blockStoreAddr, succ); err != nil {
							log.Panic("error", err)
						}
					}
				}

				newVersion := new(int32)
				if err := client.UpdateFile(indexMetaData, newVersion); err != nil {
					fmt.Println(err)
				}

				if *newVersion == -1 {
					// remote update is unseccessful - file in remote is a higher version.
					indexMetaMap[fileName] = remoteMetaMap[fileName]

					if !isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
						// Case 6:
						if err := getBlocksAndWriteToFile(remoteMetaMap[fileName], blockStoreAddr, &client); err != nil {
							log.Panic("error: ", err)
						}
					} else if isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
						// Case 5:
						// file is deleted in remote.
						// copy the tombstone entry
						indexMetaMap[fileName] = remoteMetaMap[fileName]
						// delete the file
						delFile, _ := filepath.Abs(ConcatPath(baseDir, fileName))
						if err := os.Remove(delFile); err != nil {
							log.Panic("error:", err)
						}
					}
				}
			} else if remoteMetaMap[fileName].Version+1 > indexMetaData.Version {
				// if remote has a higher version number, update the local file and version number.
				indexMetaMap[fileName] = remoteMetaMap[fileName]

				// TODO: get blocks corresponding to this fileName from server.
				if !isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
					// Case 8:
					if err := getBlocksAndWriteToFile(remoteMetaMap[fileName], blockStoreAddr, &client); err != nil {
						log.Panic("error: ", err)
					}
				} else if isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
					// Case 7:
					// file is deleted in remote.
					// copy the tombstone entry
					indexMetaMap[fileName] = remoteMetaMap[fileName]
					delFile, _ := filepath.Abs(ConcatPath(baseDir, fileName))
					// delete the file
					if err := os.Remove(delFile); err != nil {
						log.Panic("error:", err)
					}
				}
			}
		} else if indexMetaData.Version != remoteMetaMap[fileName].Version {
			// Case 10:
			indexMetaMap[fileName].Version = remoteMetaMap[fileName].Version
		}

	}

	for fileName := range indexMetaMap {
		if fileName == DEFAULT_META_FILENAME {
			continue
		}
		_, ok := remoteMetaMap[fileName]
		if !ok {
			// put blocks from fileName to remote server if the update is not delete
			PutfileName, _ := filepath.Abs(ConcatPath(baseDir, fileName))
			if _, err := os.Stat(PutfileName); err == nil {
				// case 1a
				localHashMap, hashesIn, err := getHashFromFile(fileName, int32(blockSize), baseDir)
				if err != nil {
					log.Panic("error", err)
				}
				hashesPut := make([]string, 0)
				if err := client.HasBlocks(hashesIn, blockStoreAddr, &hashesPut); err != nil {
					log.Panic("error", err)
				}

				succ := new(bool)
				for _, notHash := range hashesPut {
					if err := client.PutBlock(localHashMap[notHash], blockStoreAddr, succ); err != nil {
						log.Panic("error", err)
					}
				}
			}
			newVersion := new(int32)
			if err := client.UpdateFile(indexMetaMap[fileName], newVersion); err != nil {
				fmt.Println(err)
			}
			if *newVersion == -1 {
				// case 1b:
				// remote update is unseccessful - file in remote is a higher version.
				indexMetaMap[fileName] = remoteMetaMap[fileName]

				if !isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
					// Case 2b
					if err := getBlocksAndWriteToFile(remoteMetaMap[fileName], blockStoreAddr, &client); err != nil {
						log.Panic("error: ", err)
					}
				} else if isEqual(remoteMetaMap[fileName].BlockHashList, tombStone) {
					// Case 2a:
					// file is deleted in remote.
					// copy the tombstone entry
					indexMetaMap[fileName] = remoteMetaMap[fileName]
					// delete the file
					delFile, _ := filepath.Abs(ConcatPath(baseDir, fileName))
					if err := os.Remove(delFile); err != nil {
						log.Panic("error:", err)
					}
				}
			}
		} else {
			// handled above
			continue
		}
	}

	if err := WriteMetaFile(indexMetaMap, baseDir); err != nil {
		log.Panic("error", err)
	}

	// Debug: print remote file info after sync
	// get remote file info map.
	// remoteMetaMap = make(map[string]*FileMetaData)
	// if err := client.GetFileInfoMap(&remoteMetaMap); err != nil {
	// 	log.Fatal(err)
	// }
	// PrintMetaMap(remoteMetaMap)
}

func isEqual(str1, str2 []string) bool {
	return s.Join(str1, "") == s.Join(str2, "")
}

func getHashFromFile(fileName string, blockSize int32, baseDir string) (map[string]*Block, []string, error) {
	// convert file contents into a mapping between hashes and blocks
	localHashMap := make(map[string]*Block)
	// log.Println("FileName:", fileName)
	fileName, _ = filepath.Abs(ConcatPath(baseDir, fileName))
	fh, err := os.Open(fileName)
	if err != nil {
		log.Printf("Error reading file %v: %v", fileName, err)
		return nil, nil, err
	}

	localHashList := make([]string, 0)

	// fmt.Print("FileName: ", fileName)

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

func getBlocksAndWriteToFile(remoteMetaData *FileMetaData, blockStoreAddr string, client *RPCClient) error {
	// cases 1 and 2.
	// first get the hashlist corresponding to the entry.
	// next, iterate through the hashlist and get block for each hash.
	// while doing this, write the bytes to a file in sizes given by getBlock.

	filename := remoteMetaData.Filename
	filename, _ = filepath.Abs(ConcatPath(client.BaseDir, filename))

	// first create file if it isn't there.
	// log.Println("Creating file:", filename)
	fh, err_create := os.Create(filename)
	if err_create != nil {
		return err_create
	}
	defer fh.Close()

	// now get hashlist for this entry.
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
