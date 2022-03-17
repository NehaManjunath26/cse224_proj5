package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

var updatedLocalIndexFile string

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	baseDirectoryMap, err1 := createFileInfoMap(client)
	if err1 != nil {
		fmt.Println("CLIENT SYNC ERROR: Error while reading base directory contents ", err1)
		return
	}

	localIndexMap, err2 := readLocalIndexFile(client)
	if err2 != nil {
		fmt.Println("CLIENT SYNC ERROR: Error while reading local Index File ", err2)
		return
	}

	newFiles, deletedFiles, modifiedFiles := compareBaseAndLocal(baseDirectoryMap, localIndexMap)

	var serverFileInfoMap map[string]*FileMetaData
	err3 := client.GetFileInfoMap(&serverFileInfoMap)
	if err3 != nil {
		fmt.Println("CLIENT SYNC ERROR: Error while fetching remote Index File ", err3)
		return
	}

	for fileName, fileMetaDataObj := range serverFileInfoMap {
		if Contains(newFiles, fileName) {
			err4 := downloadFromServerAndWrite(client, fileMetaDataObj.Filename, baseDirectoryMap[fileName].BlockHashList, fileMetaDataObj.BlockHashList, blockStoreAddr)
			if err4 != nil {
				fmt.Println("CLIENT SYNC ERROR: Error while downloading and writing file blocks ", err4)
				return
			}
			updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
		} else if Contains(deletedFiles, fileName) {
			if fileMetaDataObj.Version == localIndexMap[fileName].Version {
				var latestVersion int32
				var tempMetaDataObj FileMetaData
				tempMetaDataObj.Filename = fileName
				tempMetaDataObj.Version = localIndexMap[fileName].Version + 1
				tempMetaDataObj.BlockHashList = []string{"0"}
				err4 := client.UpdateFile(&tempMetaDataObj, &latestVersion)
				if err4 != nil {
					fmt.Println("CLIENT SYNC ERROR: Error while updating deleted file blocks ", err4)
					return
				} else if latestVersion == -1 {
					var recentRemoteIndexMap map[string]*FileMetaData
					err4 = client.GetFileInfoMap(&recentRemoteIndexMap)
					if err4 != nil {
						fmt.Println("CLIENT SYNC ERROR: Error while fetching recent remote Index File ", err4)
						return
					}
					fileMetaDataObj = recentRemoteIndexMap[fileName]
					err4 := downloadDeletedFromServer(client, fileName, fileMetaDataObj.BlockHashList, blockStoreAddr)
					if err4 != nil {
						fmt.Println("CLIENT SYNC ERROR: Error while downloading and re-creating deleted file blocks ", err4)
						return
					}
					updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
				} else {
					updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(latestVersion)) + ",0" + "\n"
				}
			} else {
				err4 := downloadDeletedFromServer(client, fileName, fileMetaDataObj.BlockHashList, blockStoreAddr)
				if err4 != nil {
					fmt.Println("CLIENT SYNC ERROR: Error while downloading and re-creating deleted file blocks ", err4)
					return
				}
				updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
			}
		} else if Contains(modifiedFiles, fileName) {
			if fileMetaDataObj.Version == localIndexMap[fileName].Version {
				blockHashList, err4 := uploadFile(client, fileName, blockStoreAddr)
				if err4 != nil {
					fmt.Println("CLIENT SYNC ERROR: Error while uploading file to server ", err4)
					return
				}
				var latestVersion int32
				var tempMetaDataObj FileMetaData
				tempMetaDataObj.Filename = fileName
				tempMetaDataObj.Version = localIndexMap[fileName].Version + 1
				tempMetaDataObj.BlockHashList = blockHashList
				err4 = client.UpdateFile(&tempMetaDataObj, &latestVersion)
				if err4 != nil {
					fmt.Println("CLIENT SYNC ERROR: Error while updating modified file blocks ", err4)
					return
				} else if latestVersion == -1 {
					var recentRemoteIndexMap map[string]*FileMetaData
					err4 = client.GetFileInfoMap(&recentRemoteIndexMap)
					if err4 != nil {
						fmt.Println("CLIENT SYNC ERROR: Error while fetching recent remote Index File ", err4)
						return
					}
					fileMetaDataObj = recentRemoteIndexMap[fileName]
					err4 = downloadFromServerAndWrite(client, fileName, baseDirectoryMap[fileName].BlockHashList, fileMetaDataObj.BlockHashList, blockStoreAddr)
					if err4 != nil {
						fmt.Println("CLIENT SYNC ERROR: Error while downloading and writing file blocks ", err4)
						return
					}
					updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
				} else {
					updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(latestVersion)) + "," + strings.Join(blockHashList, " ") + "\n"
				}
			} else {
				err4 := downloadFromServerAndWrite(client, fileName, baseDirectoryMap[fileName].BlockHashList, fileMetaDataObj.BlockHashList, blockStoreAddr)
				if err4 != nil {
					fmt.Println("CLIENT SYNC ERROR: Error while downloading and writing file blocks ", err4)
					return
				}
				updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
			}
		} else {
			if _, ok := baseDirectoryMap[fileName]; ok {
				if fileMetaDataObj.Version == localIndexMap[fileName].Version {
					updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
					continue
				}
				err4 := downloadFromServerAndWrite(client, fileName, baseDirectoryMap[fileName].BlockHashList, fileMetaDataObj.BlockHashList, blockStoreAddr)
				if err4 != nil {
					fmt.Println("CLIENT SYNC ERROR: Error while downloading and writing un-modified local file blocks ", err4)
					return
				}
				updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
			} else {
				err4 := downloadDeletedFromServer(client, fileName, fileMetaDataObj.BlockHashList, blockStoreAddr)
				if err4 != nil {
					fmt.Println("CLIENT SYNC ERROR: Error while downloading and creating new local file blocks ", err4)
					return
				}
				updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
			}
		}
	}

	for fileName := range baseDirectoryMap {
		if _, ok := serverFileInfoMap[fileName]; ok {
			continue
		}
		blockHashList, err5 := uploadFile(client, fileName, blockStoreAddr)
		if err5 != nil {
			fmt.Println("CLIENT SYNC ERROR: Error while uploading file to server ", err5)
			return
		}
		var latestVersion int32
		var tempMetaDataObj FileMetaData
		var fileMetaDataObj FileMetaData
		tempMetaDataObj.Filename = fileName
		tempMetaDataObj.Version = 1
		tempMetaDataObj.BlockHashList = blockHashList
		err5 = client.UpdateFile(&tempMetaDataObj, &latestVersion)
		if err5 != nil {
			fmt.Println("CLIENT SYNC ERROR: Error while updating new baseDir file blocks ", err5)
			return
		} else if latestVersion == -1 {
			var recentRemoteIndexMap map[string]*FileMetaData
			err5 = client.GetFileInfoMap(&recentRemoteIndexMap)
			if err5 != nil {
				fmt.Println("CLIENT SYNC ERROR: Error while fetching recent remote Index File ", err5)
				return
			}
			fileMetaDataObj = *recentRemoteIndexMap[fileName]
			err5 = downloadFromServerAndWrite(client, fileName, baseDirectoryMap[fileName].BlockHashList, fileMetaDataObj.BlockHashList, blockStoreAddr)
			if err5 != nil {
				fmt.Println("CLIENT SYNC ERROR: Error while downloading and writing baseDir file blocks which got recently created in server ", err5)
				return
			}
			updatedLocalIndexFile += fileName + "," + strconv.Itoa(int(fileMetaDataObj.Version)) + "," + strings.Join(fileMetaDataObj.BlockHashList, " ") + "\n"
		} else {
			updatedLocalIndexFile += fileName + "," + "1" + "," + strings.Join(blockHashList, " ") + "\n"
		}
	}

	indexTxt, err6 := os.OpenFile(client.BaseDir+"/index.txt", os.O_WRONLY, os.ModeAppend)
	if err6 != nil {
		indexTxt, err6 = os.Create(client.BaseDir + "/index.txt")
		if err6 != nil {
			fmt.Println("CLIENT SYNC ERROR: Error while trying to create a new index.txt file ", err6)
			return
		}
	}
	err6 = indexTxt.Truncate(0)
	if err6 != nil {
		fmt.Println("CLIENT SYNC ERROR: Error while trying to truncate index.txt file for replacing ", err6)
		indexTxt.Close()
		return
	}
	_, err6 = indexTxt.WriteString(updatedLocalIndexFile)
	if err6 != nil {
		fmt.Println("CLIENT SYNC ERROR: Error while overwriting index.txt file ", err6)
		indexTxt.Close()
		return
	}
	indexTxt.Close()
}

func createFileInfoMap(client RPCClient) (map[string]FileMetaData, error) {
	baseDir := client.BaseDir
	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		fmt.Println("CLIENT ERROR: Unable to read base directory ", err)
	}

	var FileInfoMap map[string]FileMetaData = make(map[string]FileMetaData)

	for _, f := range files {
		fileName := f.Name()
		if fileName == "index.txt" {
			continue
		}
		filesize := int(f.Size())
		buffer := make([]byte, client.BlockSize)
		var BlockHashList []string

		file, err := os.Open(baseDir + "/" + fileName)
		if err != nil {
			fmt.Println("CLIENT ERROR: Error while opening file ", err)
			return nil, err
		}

		for i := 0; i < filesize/client.BlockSize; i++ {
			_, err := file.Read(buffer)
			if err != nil {
				if err != io.EOF {
					fmt.Println("CLIENT ERROR: Error while reading file ", err)
					return nil, err
				}
			}
			fileHash, err := ComputeHash(buffer)
			if err != nil {
				fmt.Println("CLIENT ERROR: Error while computing hash for file block ", err)
				return nil, err
			}
			BlockHashList = append(BlockHashList, fileHash)
		}
		remaining := filesize % client.BlockSize
		if remaining > 0 {
			buffer = make([]byte, remaining)
			_, err := file.Read(buffer)
			if err != nil {
				if err != io.EOF {
					fmt.Println("CLIENT ERROR: Error while reading file's last block ", err)
					return nil, err
				}
			}
			fileHash, err := ComputeHash(buffer)
			if err != nil {
				fmt.Println("CLIENT ERROR: Error while computing hash for file's last block ", err)
				return nil, err
			}
			BlockHashList = append(BlockHashList, fileHash)
		}
		var fileMetaDataObj FileMetaData
		fileMetaDataObj.Filename = fileName
		fileMetaDataObj.Version = -1
		fileMetaDataObj.BlockHashList = BlockHashList
		FileInfoMap[fileName] = fileMetaDataObj
		file.Close()
	}
	return FileInfoMap, nil
}

func readLocalIndexFile(client RPCClient) (map[string]FileMetaData, error) {
	file, err := os.Open(client.BaseDir + "/index.txt")
	if err != nil {
		if os.IsNotExist(err) {
			emptyFile, err1 := os.Create(client.BaseDir + "/index.txt")
			if err1 != nil {
				fmt.Println("CLIENT ERROR: Error while creating empty index file ", err)
				return nil, err1
			}
			emptyFile.Close()
			return nil, nil
		}
		fmt.Println("CLIENT ERROR: Error while opening index file ", err)
		return nil, err
	}
	var FileInfoMap map[string]FileMetaData = make(map[string]FileMetaData)
	reader := bufio.NewReader(file)
	var line string
	for {
		line, err = reader.ReadString('\n')
		if (err != nil && err != io.EOF) || len(line) == 0 {
			break
		}
		line = strings.Replace(line, "\n", "", -1)
		line = strings.TrimSpace(line)
		elements := strings.Split(line, ",")
		var fileMetaDataObj FileMetaData
		fileMetaDataObj.Filename = elements[0]
		v, _ := strconv.Atoi(elements[1])
		fileMetaDataObj.Version = int32(v)
		fileMetaDataObj.BlockHashList = strings.Split(elements[2], " ")
		FileInfoMap[elements[0]] = fileMetaDataObj
	}
	file.Close()
	if len(FileInfoMap) == 0 {
		return nil, nil
	}
	return FileInfoMap, nil
}

func compareBaseAndLocal(baseDirectoryMap map[string]FileMetaData, localIndexMap map[string]FileMetaData) ([]string, []string, []string) {
	var newFiles []string
	var deletedFiles []string
	var modifiedFiles []string

	if len(baseDirectoryMap) == 0 && len(localIndexMap) == 0 {
		return nil, nil, nil
	}
	if len(baseDirectoryMap) == 0 {
		deletedFiles = getKeys(localIndexMap)
		return nil, deletedFiles, nil
	}
	if len(localIndexMap) == 0 {
		newFiles = getKeys(baseDirectoryMap)
		return newFiles, nil, nil
	}

	for fileName := range baseDirectoryMap {
		if file, ok := localIndexMap[fileName]; ok {
			hashListBaseDir := baseDirectoryMap[fileName].BlockHashList
			hashListLocalIndex := file.BlockHashList
			if !isEquals(hashListBaseDir, hashListLocalIndex) {
				modifiedFiles = append(modifiedFiles, fileName)
			}
		} else {
			newFiles = append(newFiles, fileName)
		}
	}

	for fileName := range localIndexMap {
		if _, ok := baseDirectoryMap[fileName]; ok {

		} else if localIndexMap[fileName].BlockHashList[0] != "0" {
			deletedFiles = append(deletedFiles, fileName)
		}
	}

	return newFiles, deletedFiles, modifiedFiles
}

func uploadFile(client RPCClient, fileName string, blockStoreAddr string) ([]string, error) {
	file, err := os.Open(client.BaseDir + "/" + fileName)
	if err != nil {
		fmt.Println("CLIENT ERROR: Error while opening file in base dir", err)
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		fmt.Println("CLIENT ERROR: Error while doing stat for file in base dir", err)
		return nil, err
	}
	BufferSize := client.BlockSize
	filesize := int(fi.Size())
	buffer := make([]byte, BufferSize)
	var block Block
	var succ bool
	var blockHashList []string
	for i := 0; i < filesize/BufferSize; i++ {
		_, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("CLIENT ERROR: Error while reading file into the buffer ", err)
				file.Close()
				return nil, err
			}
		}
		block.BlockData = buffer
		block.BlockSize = int32(BufferSize)
		blockHash, _ := ComputeHash(buffer)
		blockHashList = append(blockHashList, blockHash)
		err = client.PutBlock(&block, blockStoreAddr, &succ)
		if err != nil {
			fmt.Println("CLIENT ERROR: Error while putting block to the server ", err)
			file.Close()
			return nil, err
		}
	}
	buffer = make([]byte, filesize%BufferSize)
	_, err = file.Read(buffer)
	if err != nil {
		if err != io.EOF {
			fmt.Println("CLIENT ERROR: Error while reading file's last block into the buffer ", err)
			file.Close()
			return nil, err
		}
		file.Close()
		return blockHashList, nil
	}
	block.BlockData = buffer
	block.BlockSize = int32(filesize % BufferSize)
	blockHash, _ := ComputeHash(buffer)
	blockHashList = append(blockHashList, blockHash)
	err = client.PutBlock(&block, blockStoreAddr, &succ)
	if err != nil {
		fmt.Println("CLIENT ERROR: Error while putting block to the server ", err)
		file.Close()
		return nil, err
	}
	file.Close()
	return blockHashList, nil
}

func downloadDeletedFromServer(client RPCClient, fileName string, remoteBlockHashList []string, blockStoreAddr string) error {
	if len(remoteBlockHashList) == 1 && remoteBlockHashList[0] == "0" {
		return nil
	}

	file, err := os.Create(client.BaseDir + "/" + fileName)
	if err != nil {
		fmt.Println("CLIENT ERROR: Error while re-creating deleted file in base dir ", err)
		return err
	}
	var block Block
	for i := 0; i < len(remoteBlockHashList); i++ {
		rBlockHash := remoteBlockHashList[i]
		client.GetBlock(rBlockHash, blockStoreAddr, &block)
		offset := int64(client.BlockSize * i)
		_, err = file.WriteAt(block.BlockData, offset)
		if err != nil {
			fmt.Println("CLIENT ERROR: Error while writing file block in base dir", err)
			file.Close()
			return err
		}
	}
	file.Close()
	return nil
}

func downloadFromServerAndWrite(client RPCClient, fileName string,
	baseDirBlockHashList []string, remoteBlockHashList []string, blockStoreAddr string) error {
	if len(remoteBlockHashList) == 1 && remoteBlockHashList[0] == "0" {
		err := os.Remove(client.BaseDir + "/" + fileName)
		if err != nil {
			fmt.Println("CLIENT ERROR: Error while deleting the (tombstone)file in base dir ", err)
			return err
		}
		return nil
	}

	file, err := os.OpenFile(client.BaseDir+"/"+fileName, os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Println("CLIENT ERROR: Error while opening file in base dir", err)
		return err
	}

	minLen := -1
	truncateFile := false
	var block Block
	var i int
	if len(baseDirBlockHashList) >= len(remoteBlockHashList) {
		truncateFile = true
		minLen = len(remoteBlockHashList)
	} else {
		minLen = len(baseDirBlockHashList)
	}
	for i = 0; i < minLen; i++ {
		rBlockHash := remoteBlockHashList[i]
		bBlockHash := baseDirBlockHashList[i]
		if rBlockHash != bBlockHash {
			client.GetBlock(rBlockHash, blockStoreAddr, &block)
			offset := int64(client.BlockSize * i)
			_, err = file.WriteAt(block.BlockData, offset)
			if err != nil {
				fmt.Println("CLIENT ERROR: Error while writing file block in base dir", err)
				file.Close()
				return err
			}
		}
	}
	if truncateFile {
		offset := int64((i-1)*client.BlockSize + int(block.BlockSize))
		err = os.Truncate(client.BaseDir+"/"+fileName, offset)
		if err != nil {
			fmt.Println("CLIENT ERROR: Error while truncating file's last block in base dir", err)
			file.Close()
			return err
		}
		file.Close()
		return nil
	}
	for j := i; j < len(remoteBlockHashList); j++ {
		rBlockHash := remoteBlockHashList[j]
		client.GetBlock(rBlockHash, blockStoreAddr, &block)
		offset := int64(client.BlockSize * j)
		_, err = file.WriteAt(block.BlockData, offset)
		if err != nil {
			fmt.Println("CLIENT ERROR: Error while writing additional file block in base dir", err)
			file.Close()
			return err
		}
	}
	file.Close()
	return nil
}

func isEquals(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func Contains(list []string, x string) bool {
	for _, item := range list {
		if item == x {
			return true
		}
	}
	return false
}

func ComputeHash(block []byte) (string, error) {
	hash := sha256.Sum256(block)
	sha256_hash := hex.EncodeToString(hash[:])
	return sha256_hash, nil
}

func getKeys(data map[string]FileMetaData) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	return keys
}
