package storage

import (
	"os"
	"syscall"
)

func increaseFileSize(file *os.File, size int) error {
	return syscall.Fallocate(int(file.Fd()), 0, 0, int64(size))
}

func mapFileToMemory(file *os.File, offset int64, size int) (data []byte, err error) {
	return syscall.Mmap(int(file.Fd()), offset, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}
