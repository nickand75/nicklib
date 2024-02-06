package autoupdate

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	cAutoUpdateHashSize = 128
)

type UpdFileInfo struct {
	FileName      string
	Dir           string
	RemoteDir     string
	NeedRestart   bool
	RestartLater  bool
	SetExec       bool
	DontCheckHash bool
	SilentMode    bool
}

type AutoUpdater struct {
	hashURLs  []string
	loadURLs  []string
	tasks     []UpdFileInfo
	isWorking sync.Mutex
}

func NewAutoUpdater(hashURLs []string, loadURLs []string, tasks []UpdFileInfo) *AutoUpdater {
	u := &AutoUpdater{
		hashURLs: hashURLs,
		loadURLs: loadURLs,
		tasks:    tasks,
	}

	u.update()
	go u.work()

	return u
}

func (u *AutoUpdater) AddTask(task UpdFileInfo) {
	u.isWorking.Lock()
	defer u.isWorking.Unlock()
	u.tasks = append(u.tasks, task)
	u.update()
}

func (u *AutoUpdater) AddTaskNoWait(task UpdFileInfo) {
	u.isWorking.Lock()
	defer u.isWorking.Unlock()
	u.tasks = append(u.tasks, task)
}

func (u *AutoUpdater) AddTaskList(task []UpdFileInfo) {
	u.isWorking.Lock()
	defer u.isWorking.Unlock()
	u.tasks = append(u.tasks, task...)
}

func (u *AutoUpdater) DelTask(remoteDir string, fileName string) {
	u.isWorking.Lock()
	defer u.isWorking.Unlock()
	for i := 0; i < len(u.tasks); i++ {
		if u.tasks[i].FileName == fileName && u.tasks[i].RemoteDir == remoteDir {
			u.tasks[i] = u.tasks[len(u.tasks)-1]
			u.tasks = u.tasks[:len(u.tasks)-1]
			break
		}
	}
}

func (u *AutoUpdater) work() {
	for {
		time.Sleep(1 * time.Minute)

		u.isWorking.Lock()
		u.update()
		u.isWorking.Unlock()

	}
}

func (u *AutoUpdater) update() {

	restartFlag := false
	for i := 0; i < len(u.tasks); i++ {
		if UpdateFile(u.hashURLs, u.loadURLs, u.tasks[i].Dir, u.tasks[i].RemoteDir, u.tasks[i].FileName, u.tasks[i].SetExec, u.tasks[i].DontCheckHash, u.tasks[i].SilentMode) {
			if u.tasks[i].NeedRestart {
				if !u.tasks[i].SilentMode {
					log.Println("Update file ", u.tasks[i].FileName, " completed, exiting...")
				}
				syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			} else {
				if !u.tasks[i].SilentMode {
					log.Println("Update file ", u.tasks[i].FileName, " completed.")
				}
				restartFlag = restartFlag || u.tasks[i].RestartLater
			}
		}
	}

	if restartFlag {
		log.Println("Update completed. Exiting...")
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	}
	return
}

func UpdateFile(hashURLs []string, loadURLs []string, dir string, remoteDir string, fileName string, isExec bool, dontCheckHash bool, silentMode bool) bool {

	curFilePathName := filepath.Join(dir, fileName)

	// get hash from remote
	remoteMd5Hash := doGetHash(hashURLs, remoteDir, fileName)

	if len(remoteMd5Hash) == 0 {
		return false
	}

	exists := false
	if _, err := os.Stat(curFilePathName); err == nil {
		if !dontCheckHash {
			// get currenct hash
			curMd5Hash, err := ComputeMd5File(curFilePathName)
			if err != nil {
				return false
			}

			if remoteMd5Hash == curMd5Hash {
				return false
			}
			exists = true
		}
	}

	var (
		err error
		ok  bool
	)

	for k := 0; k < 3; k++ {
		for i := 0; i < len(loadURLs); i++ {
			if ok, err = doLoadFile(loadURLs[i], remoteDir, curFilePathName, fileName, remoteMd5Hash, exists, isExec, dontCheckHash, silentMode); ok {
				return ok
			}
		}

		time.Sleep(1 * time.Minute)
	}

	if err != nil {
		log.Println(err.Error())
		return false
	}

	/*for i := 0; i < 18; i++ {
		ok, err = doLoadFile(loadURL, remoteDir, curFilePathName, fileName, remoteMd5Hash, exists, isExec, dontCheckHash, silentMode)
		if ok {
			return ok
		}
		if err != nil {
			log.Println(err.Error())
			return false
		}

		time.Sleep(10 * time.Second)
	}
	if err != nil {
		log.Println(err.Error())
		return false
	}
	log.Println("Load file error!")*/

	return false
}

func doGetHash(hashURLs []string, remoteDir string, fileName string) (res string) {

	var err error
	for k := 0; k < 3; k++ {
		for i := 0; i < len(hashURLs); i++ {
			res, err = GetRemoteMd5Hash(hashURLs[i] + remoteDir + "/" + fileName)
			if len(res) < 5 || res[:5] == "Error" || err != nil {
				res = ""
			} else {
				return
			}
		}
		time.Sleep(1 * time.Minute)

	}
	if err != nil {
		log.Println("Cannot get remote hash:", err.Error())
	}
	return
}

func doLoadFile(loadURL string, remoteDir string, curFilePathName string, fileName string, remoteMd5Hash string, exists bool, isExec bool, dontCheckHash bool, silentMode bool) (bool, error) {

	if !silentMode {
		log.Println("New version detected. Trying to download:", remoteDir+"/"+fileName)
	}

	// create HTTP request
	client := http.Client{}
	req, err := http.NewRequest("GET", loadURL+remoteDir+"/"+fileName, nil)
	if err != nil {
		return false, err
	}
	res, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 400:
		return false, errors.New("Server reply: " + strconv.Itoa(res.StatusCode))

	case 500:
		return false, nil
	}

	// create file
	if _, err := os.Stat(curFilePathName + ".new"); err == nil {
		if err := os.Remove(curFilePathName + ".new"); err != nil {
			return false, err
		}
	}

	newFile, err := os.OpenFile(curFilePathName+".new", os.O_RDWR|os.O_CREATE|os.O_APPEND, os.FileMode(0644))
	if err != nil {
		return false, err
	}

	if fileSize, err := io.Copy(newFile, res.Body); err != nil {
		return false, err
	} else {
		if !silentMode {
			log.Println("Downloaded ", fileSize)
		}
	}

	if !dontCheckHash {
		if newMd5Hash, err := ComputeMd5File(curFilePathName + ".new"); err != nil {
			return false, err
		} else {
			if newMd5Hash != remoteMd5Hash {
				return false, errors.New("Md5 hash is incorrect")
			}
		}
	}

	if isExec {
		if err := os.Chmod(curFilePathName+".new", os.FileMode(0755)); err != nil {
			return false, err
		}
	}

	if exists {
		if err := os.Rename(curFilePathName, curFilePathName+".old"); err != nil {
			return false, err
		}
	}
	if err := os.Rename(curFilePathName+".new", curFilePathName); err != nil {
		return false, err
	}

	return true, nil
}

func GetRemoteMd5Hash(checkURL string) (string, error) {

	client := http.Client{}
	// create HTTP request
	req, err := http.NewRequest("GET", checkURL, nil)
	if err != nil {
		//log.Println(err.Error())
		return "", err
	}

	resp, err := client.Do(req)
	if err != nil {
		//log.Println(err.Error(), checkURL)
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		//log.Println("Server reply: " + strconv.Itoa(resp.StatusCode))
		return "", err
	}

	// read hash
	data := make([]byte, cAutoUpdateHashSize)
	n, _ := io.ReadAtLeast(resp.Body, data, len(data))

	return string(data[:n]), nil
}

func ComputeMd5File(filePath string) (result string, err error) {

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	md5Hash := md5.New()
	if _, err := io.Copy(md5Hash, file); err != nil {
		return "", err
	}

	result = hex.EncodeToString(md5Hash.Sum(nil))

	return
}
