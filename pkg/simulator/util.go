package simulator

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

// IsEmpty
//Check if the given folder is empty/*
func IsEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

/*
Copy one file from src to dst
*/
func copy(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

// ExtractAllLogs
//Extract log files from their separate folders/*
func ExtractAllLogs(path string) error {
	rootDirectory := path
	files, err := ioutil.ReadDir(rootDirectory)
	if err != nil {
		fmt.Println(err)
		return err
	}

	for _, f := range files {
		separateDirectory := rootDirectory + "/" + f.Name()
		logFiles, err1 := ioutil.ReadDir(separateDirectory)
		if err1 != nil {
			fmt.Println(err1)
			return err1
		}
		for index, f1 := range logFiles {
			err2 := copy(separateDirectory+"/"+f1.Name(), rootDirectory+"/"+f.Name()+"_"+strconv.Itoa(
				index))
			if err2 != nil {
				fmt.Println(err2)
				return err2
			}
		}
		err3 := os.RemoveAll(separateDirectory)
		if err3 != nil {
			fmt.Println(err3)
			return err3
		}
	}
	return nil
}

// ReadAllFiles
// read all the files in 'path' as byte arrays
///*
func ReadAllFiles(path string) ([][]byte, error) {
	files, err := ioutil.ReadDir(path)
	var result [][]byte
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	for _, file := range files {
		content, err1 := os.ReadFile(path + "/" + file.Name())
		if err1 != nil {
			fmt.Println(err1)
			return nil, err1
		}
		result = append(result, content)
	}
	return result, nil
}
