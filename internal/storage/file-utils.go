package storage

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func increaseFileSize(file *os.File, allocatedFileSize int64) error {
	fileStat, err := file.Stat()

	if err != nil {
		return err
	}

	newFileSize := fileStat.Size() + allocatedFileSize

	err = unix.FcntlFstore(file.Fd(), unix.F_ALLOCATECONTIG, &unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG,
		Posmode: unix.F_PEOFPOSMODE,
		Length:  allocatedFileSize,
	})

	if err != nil {
		return err
	}

	return file.Truncate(newFileSize)
}

func mapFileToMemory(file *os.File, offset int64, size int) (data []byte, err error) {
	return syscall.Mmap(int(file.Fd()), int64(offset), int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}
