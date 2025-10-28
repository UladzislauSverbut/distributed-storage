package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

func increaseFileSize(file *os.File, allocatedFileSize int64) error {

	err := unix.FcntlFstore(file.Fd(), unix.F_ALLOCATECONTIG, &unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG,
		Posmode: unix.F_PEOFPOSMODE,
		Length:  allocatedFileSize,
	})

	if err != nil {
		return err
	}

	return file.Truncate(allocatedFileSize)
}

func mapFileToMemory(file *os.File, offset int64, size int) (data []byte, err error) {
	return unix.Mmap(int(file.Fd()), offset, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
}
