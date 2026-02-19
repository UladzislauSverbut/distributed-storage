package helpers

import (
	"os"
	"syscall"
)

func IncreaseFileSize(file *os.File, allocatedFileSize int) (int, error) {
	if err := syscall.Fallocate(int(file.Fd()), 0, 0, int64(allocatedFileSize)); err != nil {
		return 0, err
	}

	return allocatedFileSize, nil
}

func MapFileToMemory(file *os.File, offset int64, size int) (data []byte, err error) {
	return syscall.Mmap(int(file.Fd()), int64(offset), int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}
